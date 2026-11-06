"""Data warehouse -> graphiques du notebook, publiés dans graphs/.

Pour chaque graphique de commun/graphiques.py (le même code que dans Jupyter) :
  <nom>.html : version WebGL interactive, plotly.js inclus (fonctionne sans internet), affichée par le site Flask
  <nom>.png  : version image, pour le PDF
"""
import os
from pathlib import Path

import duckdb
import pendulum
from airflow.sdk import dag, task
from graphiques import GRAPHIQUES  # commun/graphiques.py (via PYTHONPATH), le même code que dans Jupyter

# from airflow.sdk import Asset   # déclenchement automatique après chargement_dw : décommenter ici,
#                                 # remplacer schedule=None par schedule=[Asset("dw")] ci-dessous
#                                 # et mettre @task(outlets=[Asset("dw")]) sur la tâche controler de chargement_dw

SORTIE = Path(os.environ["GRAPHS_DIR"])


@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=["speak-with-data"])
def publication_graphiques():

    @task
    def generer():
        con = duckdb.connect()
        con.execute("LOAD postgres")
        con.execute(f"ATTACH '{os.environ['DW_DSN']}' AS dw (TYPE postgres, READ_ONLY)")
        SORTIE.mkdir(parents=True, exist_ok=True)

        for nom, graphique in GRAPHIQUES.items():
            graphique(con, webgl=True).write_html(
                SORTIE / f"{nom}.html", include_plotlyjs=True, default_height="100%",
                config={"displaylogo": False, "responsive": True})
            graphique(con, webgl=False).write_image(SORTIE / f"{nom}.png", width=1200, height=600, scale=2)
            print(f"{SORTIE}/{nom}.html et {nom}.png")

    generer()


publication_graphiques()
