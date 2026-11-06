"""Data lake (S3) -> nettoyage -> data warehouse (PostgreSQL).

raw/   : fichiers bruts déposés dans le bucket au démarrage
clean/ : fichiers nettoyés, au format du data warehouse
dw     : schéma en étoile chargé depuis clean/

Chargement optimisé : schéma recréé (sql/01_dw.sql), données insérées sans index et triées par date,
puis clés et index créés en une fois (sql/02_index.sql) et statistiques calculées (VACUUM ANALYZE).
"""
import os
from pathlib import Path

import duckdb
import pendulum
import psycopg2
from airflow.sdk import dag, task

BUCKET = os.environ["S3_BUCKET"]
RAW = f"s3://{BUCKET}/raw"
CLEAN = f"s3://{BUCKET}/clean"
SQL = Path("/opt/airflow/sql")
TABLES = ("dim_temps", "dim_territoire", "dim_forme_juridique", "dim_entreprise", "fait_annonce", "fait_bilan")


def connexion():
    """DuckDB relié au bucket S3 (lecture/écriture Parquet) et au data warehouse PostgreSQL."""
    con = duckdb.connect()
    con.execute("SET memory_limit = '2GB'; SET threads = 4; SET temp_directory = '/tmp/duckdb';"
                "SET preserve_insertion_order = false")
    con.execute("LOAD httpfs; LOAD postgres")
    con.execute(f"""
        CREATE SECRET (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY"]}', SECRET '{os.environ["S3_SECRET_KEY"]}',
                       ENDPOINT '{os.environ["S3_ENDPOINT"]}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')
    """)
    con.execute(f"ATTACH '{os.environ['DW_DSN']}' AS dw (TYPE postgres)")
    return con


def executer_sql(*requetes):
    """Exécute du SQL directement dans PostgreSQL, hors transaction (nécessaire pour VACUUM)."""
    con = psycopg2.connect(os.environ["DW_DSN"])
    con.autocommit = True  # pas de "with con" : psycopg2 y ouvrirait une transaction
    try:
        with con.cursor() as cur:
            for requete in requetes:
                cur.execute(requete)
    finally:
        con.close()


@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=["speak-with-data"])
def chargement_dw():

    @task
    def verifier_sources():
        attendus = {f"{RAW}/bodacc/creations.parquet", f"{RAW}/bodacc/radiations.parquet",
                    f"{RAW}/inpi/donnees_financieres.parquet"}
        presents = {r[0] for r in connexion().execute(f"SELECT file FROM glob('{RAW}/**/*.parquet')").fetchall()}
        manquants = attendus - presents
        if manquants:
            raise FileNotFoundError(f"Fichiers absents du data lake : {sorted(manquants)}")

    @task
    def nettoyer_annonces():
        # Avis initiaux seulement (les id sont uniques) ; SIREN = 1re partie de "registre" sans espaces ;
        # motif de radiation déduit du texte de radiationaurcs.
        connexion().execute(f"""
            COPY (
                WITH src AS (
                    SELECT *, 'creation' AS type_annonce FROM read_parquet('{RAW}/bodacc/creations.parquet')
                    UNION ALL BY NAME
                    SELECT *, 'radiation' AS type_annonce FROM read_parquet('{RAW}/bodacc/radiations.parquet')
                )
                SELECT id AS annonce_id,
                       dateparution AS jour,
                       type_annonce,
                       nullif(regexp_extract(replace(split_part(registre, ',', 1), ' ', ''), '^[0-9]{{9}}$'), '') AS siren,
                       numerodepartement AS departement_code,
                       departement_nom_officiel AS departement_nom,
                       CAST(region_code AS VARCHAR) AS region_code,
                       region_nom_officiel AS region_nom,
                       nullif(regexp_extract(listepersonnes, '"typePersonne": "(pm|pp)"', 1), '') AS type_personne,
                       nullif(left(lower(trim(regexp_extract(listepersonnes, '"formeJuridique": "([^"]*)"', 1))), 150), '')
                           AS forme_juridique,
                       CASE WHEN type_annonce = 'radiation' THEN
                           CASE WHEN radiationaurcs LIKE '%Radiation d''office%' THEN 'office'
                                WHEN radiationaurcs ILIKE '%liquidation%' THEN 'liquidation'
                                ELSE 'autre' END
                       END AS motif_radiation
                FROM src
                WHERE typeavis = 'annonce'
            ) TO '{CLEAN}/annonces.parquet' (FORMAT parquet)
        """)

    @task
    def nettoyer_bilans():
        # Comptes sociaux uniquement (C complet, S simplifié), un bilan par SIREN et par exercice (C prioritaire).
        # Régime normal : FL = CA, HN = résultat, HK = impôt ; simplifié : 210+214+218 = CA, 310 = résultat, 306 = impôt.
        # Résultat publié mais ligne d'impôt vide -> impôt = 0.
        connexion().execute(f"""
            COPY (
                SELECT siren,
                       date_cloture_exercice AS date_cloture,
                       type_bilan,
                       confidentiality AS confidentialite,
                       CASE type_bilan WHEN 'C' THEN liasse['FL']::BIGINT
                            ELSE nullif(coalesce(liasse['210'], 0)::BIGINT + coalesce(liasse['214'], 0)
                                        + coalesce(liasse['218'], 0), 0)
                       END AS chiffre_affaires,
                       (CASE type_bilan WHEN 'C' THEN liasse['HN'] ELSE liasse['310'] END)::BIGINT AS resultat,
                       CASE WHEN (CASE type_bilan WHEN 'C' THEN liasse['HN'] ELSE liasse['310'] END) IS NOT NULL
                            THEN coalesce(CASE type_bilan WHEN 'C' THEN liasse['HK'] ELSE liasse['306'] END, 0)::BIGINT
                       END AS impot_benefices
                FROM read_parquet('{RAW}/inpi/donnees_financieres.parquet')
                WHERE type_bilan IN ('C', 'S')
                  AND date_cloture_exercice BETWEEN DATE '2000-01-01' AND DATE '2030-12-31'
                  AND regexp_full_match(siren, '[0-9]{{9}}')
                QUALIFY row_number() OVER (PARTITION BY siren, date_cloture_exercice ORDER BY type_bilan) = 1
            ) TO '{CLEAN}/bilans.parquet' (FORMAT parquet)
        """)

    @task
    def creer_schema():
        # Tables recréées vides et sans index : le DAG peut être relancé sans doublons, et l'insertion
        # ne met à jour aucun index.
        executer_sql((SQL / "01_dw.sql").read_text(encoding="utf-8"))

    @task
    def charger_dimensions():
        con = connexion()
        con.execute("""
            INSERT INTO dw.dim_temps
            SELECT d::DATE, year(d), quarter(d), month(d)
            FROM range(DATE '2000-01-01', DATE '2031-01-01', INTERVAL 1 DAY) t(d)
        """)
        con.execute(f"""
            INSERT INTO dw.dim_territoire
            SELECT departement_code, any_value(departement_nom), any_value(region_code), any_value(region_nom)
            FROM read_parquet('{CLEAN}/annonces.parquet')
            WHERE departement_code IS NOT NULL
            GROUP BY departement_code
        """)
        con.execute(f"""
            INSERT INTO dw.dim_forme_juridique
            SELECT row_number() OVER (ORDER BY forme_juridique), forme_juridique
            FROM (SELECT DISTINCT forme_juridique FROM read_parquet('{CLEAN}/annonces.parquet')
                  WHERE forme_juridique IS NOT NULL)
        """)
        # Une ligne par SIREN : infos de l'annonce la plus récente, plus les SIREN connus uniquement par leurs bilans.
        con.execute(f"""
            INSERT INTO dw.dim_entreprise
            SELECT e.siren::INTEGER, f.forme_juridique_id, e.type_personne
            FROM (
                SELECT siren,
                       arg_max(type_personne, jour) FILTER (WHERE type_personne IS NOT NULL) AS type_personne,
                       arg_max(forme_juridique, jour) FILTER (WHERE forme_juridique IS NOT NULL) AS forme_juridique
                FROM (
                    SELECT siren, type_personne, forme_juridique, jour
                    FROM read_parquet('{CLEAN}/annonces.parquet') WHERE siren IS NOT NULL
                    UNION ALL
                    SELECT DISTINCT siren, NULL, NULL, NULL FROM read_parquet('{CLEAN}/bilans.parquet')
                )
                GROUP BY siren
            ) e
            LEFT JOIN dw.dim_forme_juridique f ON f.libelle = e.forme_juridique
        """)

    # Faits insérés triés par date : les lignes d'une même période sont voisines sur le disque,
    # ce qui rend l'index BRIN efficace (preserve_insertion_order pour que DuckDB garde l'ordre).
    @task
    def charger_fait_annonce():
        con = connexion()
        con.execute("SET preserve_insertion_order = true")
        con.execute(f"""
            INSERT INTO dw.fait_annonce
            SELECT CAST(CASE left(annonce_id, 1) WHEN 'A' THEN '1' WHEN 'B' THEN '2' END || substr(annonce_id, 2)
                        AS BIGINT),
                   siren::INTEGER, jour, type_annonce, motif_radiation, departement_code
            FROM read_parquet('{CLEAN}/annonces.parquet')
            WHERE jour BETWEEN DATE '2000-01-01' AND DATE '2030-12-31'
            ORDER BY jour
        """)

    @task
    def charger_fait_bilan():
        con = connexion()
        con.execute("SET preserve_insertion_order = true")
        con.execute(f"""
            INSERT INTO dw.fait_bilan
            SELECT chiffre_affaires, resultat, impot_benefices, siren::INTEGER, date_cloture, confidentialite, type_bilan
            FROM read_parquet('{CLEAN}/bilans.parquet')
            ORDER BY date_cloture
        """)

    @task
    def indexer():
        # Clés et index construits en une fois, puis VACUUM (carte de visibilité : lectures d'index seul)
        # et ANALYZE (statistiques à jour pour le planificateur).
        executer_sql((SQL / "02_index.sql").read_text(encoding="utf-8"),
                     *(f"VACUUM (ANALYZE) {t}" for t in TABLES))

    @task  # @task(outlets=[Asset("dw")]) pour déclencher publication_graphiques à la fin du chargement
    def controler():
        con = connexion()
        for table in TABLES:
            n = con.execute(f"SELECT count(*) FROM dw.{table}").fetchone()[0]
            print(f"{table:<15} {n:>12,}")
            if n == 0:
                raise ValueError(f"{table} est vide")
        # Indicateur de l'exercice 1 : sociétés créées / radiées hors radiations d'office, par année
        print(con.execute("""
            SELECT t.annee,
                   count(*) FILTER (WHERE a.type_annonce = 'creation') AS creations,
                   count(*) FILTER (WHERE a.type_annonce = 'radiation' AND a.motif_radiation <> 'office') AS radiations_hors_office
            FROM dw.fait_annonce a
            JOIN dw.dim_entreprise e USING (siren)
            JOIN dw.dim_temps t USING (jour)
            WHERE e.type_personne = 'pm'
            GROUP BY t.annee ORDER BY t.annee
        """).df().to_string(index=False))

    dims = charger_dimensions()
    verifier_sources() >> [nettoyer_annonces(), nettoyer_bilans()] >> creer_schema() >> dims
    dims >> [charger_fait_annonce(), charger_fait_bilan()] >> indexer() >> controler()


chargement_dw()
