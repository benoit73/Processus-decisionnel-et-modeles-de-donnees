-- Clés et index du data warehouse. Exécuté par le DAG chargement_dw APRÈS l'insertion des données :
-- construire un index en une fois est bien plus rapide que de le mettre à jour ligne par ligne.

ALTER TABLE dim_temps           ADD PRIMARY KEY (jour);
ALTER TABLE dim_territoire      ADD PRIMARY KEY (departement_code);
ALTER TABLE dim_forme_juridique ADD PRIMARY KEY (forme_juridique_id), ADD UNIQUE (libelle);
ALTER TABLE dim_entreprise      ADD PRIMARY KEY (siren),
                                ADD FOREIGN KEY (forme_juridique_id) REFERENCES dim_forme_juridique;
ALTER TABLE fait_annonce        ADD PRIMARY KEY (annonce_id),
                                ADD FOREIGN KEY (siren) REFERENCES dim_entreprise,
                                ADD FOREIGN KEY (jour) REFERENCES dim_temps,
                                ADD FOREIGN KEY (departement_code) REFERENCES dim_territoire;
ALTER TABLE fait_bilan          ADD PRIMARY KEY (siren, date_cloture),
                                ADD FOREIGN KEY (siren) REFERENCES dim_entreprise,
                                ADD FOREIGN KEY (date_cloture) REFERENCES dim_temps;

-- Accès à l'historique d'une entreprise
CREATE INDEX ON fait_annonce (siren);

-- Les faits sont insérés triés par date : un index BRIN (min/max par bloc de pages) suffit pour les
-- filtres sur une période, et pèse quelques Ko au lieu de ~100 Mo pour un B-tree.
CREATE INDEX ON fait_annonce USING brin (jour);
CREATE INDEX ON fait_bilan   USING brin (date_cloture);
