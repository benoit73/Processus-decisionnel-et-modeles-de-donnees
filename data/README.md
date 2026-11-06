# Données brutes

Les fichiers ne sont pas versionnés (5 Go). Les télécharger au format Parquet et les placer ici **avec exactement ces noms** : le `docker compose` de l'exercice 3 les cherche sous ces noms.

| Nom du fichier | Source | Filtre |
|---|---|---|
| `bodacc creation.parquet` | [BODACC, annonces commerciales](https://www.bodacc.fr/explore/dataset/annonces-commerciales/export/?sort=dateparution) | Famille d'avis = Créations |
| `bodacc radiations.parquet` | [BODACC, annonces commerciales](https://www.bodacc.fr/explore/dataset/annonces-commerciales/export/?sort=dateparution) | Famille d'avis = Radiations |
| `données financières.parquet` | [Données financières détaillées des entreprises](https://www.data.gouv.fr/datasets/donnees-financieres-detaillees-des-entreprises-format-parquet) | aucun |

Attention : le site du BODACC coupe les gros exports (vers 1,6 Go) sans prévenir. Un fichier coupé ne se termine pas par `PAR1` et ne peut pas être lu. Or les créations font environ 2,2 Go et les radiations 1,4 Go : l'export en une fois depuis le site échoue.

Pour les deux fichiers BODACC, utiliser plutôt le script (nécessite `pip install duckdb`) :

```bash
python telecharger_bodacc.py
```

Il télécharge chaque année séparément par l'API (`bodacc_annees/<famille>/<annee>.parquet`, 70 à 180 Mo chacun), vérifie chaque morceau (`PAR1` en fin de fichier, nombre de lignes égal à celui annoncé par l'API), puis les fusionne sous les noms ci-dessus. Relancé, il ne retélécharge que les années manquantes ou incomplètes.
