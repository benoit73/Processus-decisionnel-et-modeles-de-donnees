"""Télécharge les annonces BODACC (créations, radiations) année par année, puis les fusionne.

Le site du BODACC coupe les exports vers 1,6 Go : un export complet donne un Parquet tronqué.
Une année fait 70 à 180 Mo, bien en dessous.

  bodacc_annees/<famille>/<annee>.parquet : un fichier par année (conservés, reprise possible)
  bodacc creation.parquet, bodacc radiations.parquet : fichiers fusionnés, lus par l'exercice 3

Chaque morceau est vérifié (signature PAR1 en fin de fichier, nombre de lignes égal à celui de l'API).
Relancer le script ne retélécharge que les années manquantes ou incomplètes.

Usage : python telecharger_bodacc.py   (nécessite duckdb)
"""
import json
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

API = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales"
ICI = Path(__file__).parent
FAMILLES = {"creation": "bodacc creation.parquet", "radiation": "bodacc radiations.parquet"}


def filtre(famille, annee=None):
    w = f'familleavis="{famille}"'
    if annee is not None:
        w += f" AND dateparution>=date'{annee}-01-01' AND dateparution<date'{annee + 1}-01-01'"
    return w


def api(chemin, **params):
    url = f"{API}/{chemin}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=120) as r:
        return json.load(r)


def lignes_attendues(famille):
    """{annee: nombre d'annonces} d'après l'API."""
    res = api("records", select="count(*) as n", where=filtre(famille),
              group_by="year(dateparution) as annee", order_by="annee", limit=100)["results"]
    return {int(r["annee"]): r["n"] for r in res if r["annee"] is not None}


def compter(fichier):
    with duckdb.connect() as con:
        return con.execute(f"SELECT count(*) FROM read_parquet('{fichier.as_posix()}')").fetchone()[0]


def valide(fichier, attendu):
    if not fichier.exists() or fichier.stat().st_size < 12:
        return False
    with open(fichier, "rb") as f:
        f.seek(-4, 2)
        if f.read() != b"PAR1":
            return False
    return compter(fichier) == attendu


def telecharger(famille, annee, attendu):
    fichier = ICI / "bodacc_annees" / famille / f"{annee}.parquet"
    if valide(fichier, attendu):
        return f"{famille} {annee} : déjà présent"
    fichier.parent.mkdir(parents=True, exist_ok=True)
    url = f"{API}/exports/parquet?{urllib.parse.urlencode({'where': filtre(famille, annee)})}"
    for essai in range(1, 4):
        partiel = fichier.with_suffix(".part")
        try:
            with urllib.request.urlopen(url, timeout=600) as r, open(partiel, "wb") as f:
                while bloc := r.read(1 << 20):
                    f.write(bloc)
            partiel.replace(fichier)
            if valide(fichier, attendu):
                return f"{famille} {annee} : {attendu:,} lignes, {fichier.stat().st_size / 1e6:,.0f} Mo"
        except OSError as e:
            print(f"{famille} {annee} : essai {essai} échoué ({e})")
        time.sleep(10 * essai)
    raise RuntimeError(f"{famille} {annee} : téléchargement invalide après 3 essais")


def main():
    for famille, nom in FAMILLES.items():
        attendus = lignes_attendues(famille)
        print(f"{famille} : {sum(attendus.values()):,} annonces, {len(attendus)} années")
        with ThreadPoolExecutor(max_workers=3) as pool:
            for msg in pool.map(lambda a: telecharger(famille, a, attendus[a]), sorted(attendus)):
                print(msg, flush=True)

        # Fusion en un seul fichier, au nom attendu par l'exercice 3
        morceaux = [(ICI / "bodacc_annees" / famille / f"{a}.parquet").as_posix() for a in sorted(attendus)]
        tmp = ICI / (nom + ".tmp")
        with duckdb.connect() as con:
            con.execute(f"COPY (SELECT * FROM read_parquet({morceaux}, union_by_name = true)) "
                        f"TO '{tmp.as_posix()}' (FORMAT parquet)")
        n = compter(tmp)
        if n != sum(attendus.values()):
            raise RuntimeError(f"{nom} : {n:,} lignes fusionnées au lieu de {sum(attendus.values()):,}")
        tmp.replace(ICI / nom)
        print(f"{nom} : {n:,} lignes, {(ICI / nom).stat().st_size / 1e9:.2f} Go\n", flush=True)


if __name__ == "__main__":
    main()
