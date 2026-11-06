"""Site de consultation des graphiques : sert les HTML WebGL produits par le DAG publication_graphiques."""
import os
from pathlib import Path

from flask import Flask, abort, render_template, send_from_directory

GRAPHS = Path(os.environ.get("GRAPHS_DIR", "/graphs"))
app = Flask(__name__)


@app.get("/")
def index():
    graphiques = sorted(p.stem for p in GRAPHS.glob("*.html"))
    return render_template("index.html", graphiques=graphiques)


@app.get("/graphs/<nom>.html")
def graphique(nom):
    if not (GRAPHS / f"{nom}.html").is_file():
        abort(404)
    return send_from_directory(GRAPHS, f"{nom}.html")
