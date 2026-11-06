-- Data warehouse en étoile (base dw) : 2 faits, 4 dimensions. Tables seules : clés et index dans 02_index.sql.
-- Exécuté au premier démarrage de PostgreSQL, puis par le DAG chargement_dw au début de chaque chargement.
--
-- Types choisis pour le volume : entiers plutôt que texte (SIREN, identifiant d'annonce), ENUM pour les
-- valeurs fermées, colonnes de 8 octets en tête de ligne pour limiter le remplissage d'alignement.

DROP TABLE IF EXISTS fait_annonce, fait_bilan, dim_entreprise, dim_forme_juridique, dim_territoire, dim_temps CASCADE;
DROP TYPE IF EXISTS type_annonce, motif_radiation, confidentialite;

CREATE TYPE type_annonce    AS ENUM ('creation', 'radiation');
CREATE TYPE motif_radiation AS ENUM ('office', 'liquidation', 'autre');
CREATE TYPE confidentialite AS ENUM ('Public', 'Partiellement confidentiel', 'Partiellement confidentiel (RAPCAC)',
                                     'Publication simplifiee');

CREATE TABLE dim_temps (
    jour        DATE     NOT NULL,
    annee       SMALLINT NOT NULL,
    trimestre   SMALLINT NOT NULL,
    mois        SMALLINT NOT NULL
);

CREATE TABLE dim_territoire (
    departement_code  VARCHAR(3) NOT NULL,
    departement_nom   VARCHAR(100),
    region_code       VARCHAR(3),
    region_nom        VARCHAR(100)
);

-- ~1 800 formes juridiques distinctes : stockées une fois au lieu d'être répétées sur 9 M d'entreprises
CREATE TABLE dim_forme_juridique (
    forme_juridique_id  SMALLINT     NOT NULL,
    libelle             VARCHAR(150) NOT NULL
);

CREATE TABLE dim_entreprise (
    siren               INTEGER  NOT NULL,   -- 9 chiffres : tient dans un entier (afficher avec lpad(siren::text, 9, '0'))
    forme_juridique_id  SMALLINT,
    type_personne       VARCHAR(2)           -- pm : société, pp : entrepreneur individuel
);

CREATE TABLE fait_annonce (
    annonce_id        BIGINT       NOT NULL, -- id BODACC, lettre remplacée par un chiffre : A2008000110 -> 12008000110, B... -> 2...
    siren             INTEGER,
    jour              DATE         NOT NULL,
    type_annonce      type_annonce NOT NULL,
    motif_radiation   motif_radiation,
    departement_code  VARCHAR(3)
);

CREATE TABLE fait_bilan (
    chiffre_affaires  BIGINT,
    resultat          BIGINT,
    impot_benefices   BIGINT,
    siren             INTEGER NOT NULL,
    date_cloture      DATE    NOT NULL,
    confidentialite   confidentialite,
    type_bilan        "char"  NOT NULL CHECK (type_bilan IN ('C', 'S'))   -- "char" : 1 octet
);
