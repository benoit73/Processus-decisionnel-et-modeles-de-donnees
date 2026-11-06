# Image commune à Airflow et Jupyter : Airflow + DuckDB (lecture S3, écriture PostgreSQL) + JupyterLab + Plotly
FROM apache/airflow:3.3.2

# Chromium : kaleido s'en sert pour exporter les graphiques Plotly en PNG
USER root
RUN apt-get update && apt-get install -y --no-install-recommends chromium && rm -rf /var/lib/apt/lists/*
ENV BROWSER_PATH=/usr/bin/chromium
USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" duckdb==1.5.* jupyterlab plotly==6.* kaleido==1.*

# Extensions DuckDB téléchargées à la construction plutôt qu'à chaque exécution
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs; INSTALL postgres')"
