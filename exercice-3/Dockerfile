# Image commune à Airflow et Jupyter : Airflow + DuckDB (lecture S3, écriture PostgreSQL) + JupyterLab + Plotly + Quarto
FROM apache/airflow:3.3.2

# Chromium : kaleido s'en sert pour exporter les graphiques Plotly en PNG
USER root
RUN apt-get update && apt-get install -y --no-install-recommends chromium && rm -rf /var/lib/apt/lists/*
ENV BROWSER_PATH=/usr/bin/chromium

# Quarto (rapport PDF, Typst inclus : pas besoin de LaTeX)
ARG QUARTO_VERSION=1.10.19
ARG TARGETARCH
RUN curl -fsSL "https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-${TARGETARCH:-amd64}.tar.gz" \
      | tar -xz -C /opt && ln -s "/opt/quarto-${QUARTO_VERSION}/bin/quarto" /usr/local/bin/quarto
USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" duckdb==1.5.* jupyterlab plotly==6.* kaleido==1.*

# Extensions DuckDB téléchargées à la construction plutôt qu'à chaque exécution
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs; INSTALL postgres')"
