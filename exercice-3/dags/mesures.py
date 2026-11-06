"""Mesures du data warehouse : volume, temps des requêtes de référence, durée du chargement.

Ne modifie rien dans la base. Écrit dans mesures/<etiquette>/ (versionné) :
  resultats.md       : tableaux de synthèse
  plans/<requete>.txt : plan d'exécution (EXPLAIN ANALYZE) de chaque requête

Lancer avant puis après une optimisation, avec la même méthode, et comparer les deux dossiers :
  airflow dags trigger mesures                              (etiquette = avant)
  airflow dags trigger mesures -c '{"etiquette": "apres"}'
"""
import os
import statistics
from pathlib import Path

import pendulum
import psycopg2
from airflow.sdk import Param, dag, get_current_context, task

SORTIE = Path(os.environ["MESURES_DIR"])
TABLES = ("dim_temps", "dim_territoire", "dim_forme_juridique", "dim_entreprise", "fait_annonce", "fait_bilan")
EXECUTIONS = 5  # après une exécution de chauffe : on garde la médiane (cache chaud)

# Requêtes de référence : celles du projet, plus deux accès typiques (une entreprise, une période).
REQUETES = {
    "creations_radiations": ("Notebook §3 et site : sociétés créées / radiées par année", """
        SELECT t.annee,
               count(*) FILTER (WHERE a.type_annonce = 'creation') AS creations,
               count(*) FILTER (WHERE a.type_annonce = 'radiation') AS radiations,
               count(*) FILTER (WHERE a.motif_radiation = 'office') AS dont_office
        FROM fait_annonce a
        JOIN dim_entreprise e USING (siren)
        JOIN dim_temps t USING (jour)
        WHERE e.type_personne = 'pm' AND t.annee <= 2025
        GROUP BY t.annee ORDER BY t.annee
    """),
    "is_par_tranche": ("Notebook §4 et rapport : IS par tranche de CA (2023)", """
        SELECT CASE WHEN chiffre_affaires < 1e5 THEN '1. < 100 k€'
                    WHEN chiffre_affaires < 1e6 THEN '2. 100 k€ - 1 M€'
                    WHEN chiffre_affaires < 1e7 THEN '3. 1 - 10 M€'
                    WHEN chiffre_affaires < 5e7 THEN '4. 10 - 50 M€'
                    ELSE '5. >= 50 M€' END AS tranche_ca,
               count(*) AS entreprises,
               count(*) FILTER (WHERE impot_benefices > 0) AS payent_is,
               round(sum(impot_benefices) FILTER (WHERE impot_benefices > 0) / 1e9, 2) AS is_md_eur
        FROM fait_bilan b
        JOIN dim_temps t ON t.jour = b.date_cloture
        WHERE t.annee = 2023 AND chiffre_affaires > 0 AND resultat IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """),
    "indicateur_controle": ("DAG chargement_dw, tâche controler : créations / radiations hors office", """
        SELECT t.annee,
               count(*) FILTER (WHERE a.type_annonce = 'creation') AS creations,
               count(*) FILTER (WHERE a.type_annonce = 'radiation' AND a.motif_radiation <> 'office')
                   AS radiations_hors_office
        FROM fait_annonce a
        JOIN dim_entreprise e USING (siren)
        JOIN dim_temps t USING (jour)
        WHERE e.type_personne = 'pm'
        GROUP BY t.annee ORDER BY t.annee
    """),
    "creations_departement_2023": ("Une période : créations par département au 1er trimestre 2023", """
        SELECT a.departement_code, count(*) AS creations
        FROM fait_annonce a
        WHERE a.type_annonce = 'creation' AND a.jour BETWEEN DATE '2023-01-01' AND DATE '2023-03-31'
        GROUP BY 1 ORDER BY 2 DESC
    """),
    "historique_entreprise": ("Une entreprise : annonces et bilans d'un SIREN", """
        SELECT 'annonce' AS source, jour AS date, type_annonce::text AS detail FROM fait_annonce
        WHERE siren = (SELECT max(siren) FROM fait_bilan)
        UNION ALL
        SELECT 'bilan', date_cloture, type_bilan::text FROM fait_bilan
        WHERE siren = (SELECT max(siren) FROM fait_bilan)
    """),
}


def nombre(x, decimales=0):
    """Format français : 412 345 ; 41,3"""
    return f"{x:,.{decimales}f}".replace(",", " ").replace(".", ",")


def mo(octets):
    return nombre(octets / 1024**2, 1)


def ms(x):
    return nombre(x, 0 if x >= 100 else 1 if x >= 10 else 2)


def tableau(entetes, lignes, gauche=1):
    """Tableau Markdown : les `gauche` premières colonnes (texte) à gauche, les nombres à droite."""
    return "\n".join(["| " + " | ".join(entetes) + " |",
                      "|" + "|".join(":--" if i < gauche else "--:" for i in range(len(entetes))) + "|",
                      *("| " + " | ".join(str(c) for c in l) + " |" for l in lignes)])


@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=["speak-with-data"],
     params={"etiquette": Param("avant", type="string", description="Sous-dossier de mesures/ : avant, apres…")})
def mesures():

    @task
    def mesurer():
        etiquette = get_current_context()["params"]["etiquette"]
        dossier = SORTIE / etiquette
        (dossier / "plans").mkdir(parents=True, exist_ok=True)
        con = psycopg2.connect(os.environ["DW_DSN"])
        con.autocommit = True
        cur = con.cursor()
        md = [f"# Mesures : {etiquette}", "",
              f"Le {pendulum.now('Europe/Paris').format('DD/MM/YYYY à HH:mm')}."]

        # Contexte : version et réglages mémoire, qui influencent les temps
        cur.execute("SELECT version()")
        version = cur.fetchone()[0].split(" on ")[0]
        reglages = []
        for nom in ("shared_buffers", "work_mem", "effective_cache_size", "max_parallel_workers_per_gather"):
            cur.execute("SELECT current_setting(%s)", [nom])  # valeur lisible : 128MB, 4MB…
            reglages.append(f"`{nom}` = {cur.fetchone()[0]}")
        reglages = ", ".join(reglages)
        md += ["", f"{version} ; {reglages}."]

        # 1. Volume
        lignes, total = [], [0, 0, 0]
        for t in TABLES:
            cur.execute(f"SELECT count(*), pg_table_size('{t}'), pg_indexes_size('{t}'), "
                        f"pg_total_relation_size('{t}') FROM {t}")
            n, donnees, index, tot = cur.fetchone()
            total = [total[0] + donnees, total[1] + index, total[2] + tot]
            lignes.append([f"`{t}`", nombre(n), mo(donnees), mo(index), mo(tot), nombre(tot / n) if n else "-"])
        lignes.append(["**Total**", "", *(f"**{mo(x)}**" for x in total), ""])
        cur.execute("SELECT pg_database_size(current_database())")
        md += ["", "## Volume", "",
               tableau(["Table", "Lignes", "Données (Mo)", "Index (Mo)", "Total (Mo)", "Octets / ligne"], lignes),
               "", f"Base `dw` entière : {mo(cur.fetchone()[0])} Mo."]

        cur.execute("""SELECT indexrelid::regclass, indrelid::regclass, pg_relation_size(indexrelid)
                       FROM pg_index WHERE indrelid::regclass::text = ANY(%s) ORDER BY 3 DESC""", [list(TABLES)])
        md += ["", "### Index", "",
               tableau(["Index", "Table", "Taille (Mo)"], [[f"`{i}`", f"`{t}`", mo(s)] for i, t, s in cur.fetchall()],
                       gauche=2)]

        # 2. Temps des requêtes : médiane de EXECUTIONS exécutions, après une chauffe
        lignes = []
        for nom, (description, sql) in REQUETES.items():
            cur.execute(sql)  # chauffe
            temps, plan = [], None
            for _ in range(EXECUTIONS):
                cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
                plan = cur.fetchone()[0][0]
                temps.append(plan["Execution Time"])
            cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
            (dossier / "plans" / f"{nom}.txt").write_text(
                f"-- {description}\n{sql.strip()}\n\n" + "\n".join(r[0] for r in cur.fetchall()) + "\n",
                encoding="utf-8")
            racine = plan["Plan"]
            pages = racine.get("Shared Hit Blocks", 0) + racine.get("Shared Read Blocks", 0)
            lignes.append([f"`{nom}`", description, ms(statistics.median(temps)), ms(min(temps)),
                           mo(pages * 8192)])
        md += ["", "## Temps des requêtes", "",
               f"Médiane de {EXECUTIONS} exécutions après une exécution de chauffe (cache chaud), "
               "temps mesuré par `EXPLAIN ANALYZE`. Plans détaillés dans `plans/`.", "",
               tableau(["Requête", "Rôle", "Médiane (ms)", "Min (ms)", "Données lues (Mo)"], lignes, gauche=2)]

        # 3. Durée du dernier chargement réussi, lue dans les métadonnées d'Airflow
        md += ["", "## Durée du chargement (dernier `chargement_dw` réussi)", ""]
        with psycopg2.connect(os.environ["AIRFLOW_DB_DSN"]) as meta, meta.cursor() as c:
            c.execute("""
                SELECT run_id, end_date - start_date FROM dag_run
                WHERE dag_id = 'chargement_dw' AND state = 'success' ORDER BY end_date DESC LIMIT 1""")
            run = c.fetchone()
            if run is None:
                md.append("Aucun chargement réussi.")
            else:
                c.execute("""SELECT task_id, duration FROM task_instance
                             WHERE dag_id = 'chargement_dw' AND run_id = %s ORDER BY start_date""", [run[0]])
                lignes = [[f"`{t}`", nombre(d)] for t, d in c.fetchall()]
                lignes.append(["**Total du DAG**", f"**{nombre(run[1].total_seconds())}**"])
                md += [f"Exécution `{run[0]}`.", "", tableau(["Tâche", "Durée (s)"], lignes)]

        (dossier / "resultats.md").write_text("\n".join(md) + "\n", encoding="utf-8")
        print("\n".join(md))

    mesurer()


mesures()
