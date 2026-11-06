"""Graphiques et tableaux validés dans Jupyter, réutilisés tels quels par les DAG
publication_graphiques (HTML + PNG) et rapport (PDF).

Chaque fonction prend une connexion DuckDB où le data warehouse est attaché sous le nom `dw`
et renvoie une figure Plotly. `webgl=True` : tracés Scattergl (affichage web interactif) ;
`webgl=False` : tracés Scatter, même graphique, pour l'export en image.
"""
import plotly.graph_objects as go

# Palette : une couleur par série, toujours dans le même ordre
BLEU, ORANGE, VERT = "#2a78d6", "#eb6834", "#1baf7a"


def creations_radiations(con, webgl=True):
    """Sociétés créées et radiées par année, avec et sans les radiations d'office."""
    annonces = con.sql("""
        SELECT * FROM postgres_query('dw', '
            SELECT t.annee,
                   count(*) FILTER (WHERE a.type_annonce = ''creation'') AS creations,
                   count(*) FILTER (WHERE a.type_annonce = ''radiation'') AS radiations,
                   count(*) FILTER (WHERE a.motif_radiation = ''office'') AS dont_office
            FROM fait_annonce a
            JOIN dim_entreprise e USING (siren)
            JOIN dim_temps t USING (jour)
            WHERE e.type_personne = ''pm'' AND t.annee <= 2025
            GROUP BY t.annee ORDER BY t.annee')
    """).df()

    Trace = go.Scattergl if webgl else go.Scatter
    series = [
        ("Créations", annonces.creations, BLEU, "solid"),
        ("Radiations (toutes)", annonces.radiations, ORANGE, "solid"),
        ("Radiations hors radiations d'office", annonces.radiations - annonces.dont_office, VERT, "dash"),
    ]
    fig = go.Figure([
        Trace(x=annonces.annee, y=y, name=nom, mode="lines+markers",
              line=dict(color=couleur, width=2, dash=trait), marker=dict(size=8),
              hovertemplate="%{y:,}<extra>" + nom + "</extra>")
        for nom, y, couleur, trait in series
    ])
    fig.update_layout(
        title="Sociétés créées et radiées par année",
        template="plotly_white", hovermode="x unified", separators=", ",
        legend=dict(orientation="h", x=1, xanchor="right", y=1.02, yanchor="bottom"),
        xaxis=dict(dtick=2),
        yaxis=dict(rangemode="tozero", title="Annonces BODACC"),
        margin=dict(l=70, r=20, t=80, b=40),
    )
    return fig


def is_par_tranche(con):
    """Impôt sur les bénéfices par tranche de chiffre d'affaires (exercices clôturés en 2023)."""
    return con.sql("""
        SELECT * FROM postgres_query('dw', '
            SELECT CASE WHEN chiffre_affaires < 1e5 THEN ''1. < 100 k€''
                        WHEN chiffre_affaires < 1e6 THEN ''2. 100 k€ - 1 M€''
                        WHEN chiffre_affaires < 1e7 THEN ''3. 1 - 10 M€''
                        WHEN chiffre_affaires < 5e7 THEN ''4. 10 - 50 M€''
                        ELSE ''5. >= 50 M€'' END AS tranche_ca,
                   count(*) AS entreprises,
                   count(*) FILTER (WHERE impot_benefices > 0) AS payent_is,
                   round(sum(impot_benefices) FILTER (WHERE impot_benefices > 0) / 1e9, 2) AS is_md_eur
            FROM fait_bilan b
            JOIN dim_temps t ON t.jour = b.date_cloture
            WHERE t.annee = 2023 AND chiffre_affaires > 0 AND resultat IS NOT NULL
            GROUP BY 1 ORDER BY 1')
    """).df()


# Graphiques publiés par le DAG : nom du fichier -> fonction
GRAPHIQUES = {
    "creations_radiations": creations_radiations,
}
