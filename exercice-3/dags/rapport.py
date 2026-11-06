"""Data warehouse -> rapport PDF (Quarto, rendu Typst) avec le graphique et le tableau du notebook.

Source : rapport/rapport.qmd, qui appelle commun/graphiques.py comme Jupyter.
Sortie : rapport/pdf/rapport.pdf
"""
import shutil
import subprocess
import tempfile
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

SOURCE = Path("/opt/airflow/rapport/rapport.qmd")
SORTIE = Path("/opt/airflow/rapport/pdf")


@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=["speak-with-data"])
def rapport():

    @task
    def rendre_pdf():
        # Quarto écrit ses fichiers intermédiaires à côté de la source : rendu dans un dossier temporaire
        with tempfile.TemporaryDirectory() as tmp:
            qmd = Path(tmp) / SOURCE.name
            shutil.copy(SOURCE, qmd)
            subprocess.run(["quarto", "render", qmd.name, "--to", "typst"], cwd=tmp, check=True)
            SORTIE.mkdir(parents=True, exist_ok=True)
            shutil.copy(qmd.with_suffix(".pdf"), SORTIE / "rapport.pdf")
        print(f"{SORTIE}/rapport.pdf")

    rendre_pdf()


rapport()
