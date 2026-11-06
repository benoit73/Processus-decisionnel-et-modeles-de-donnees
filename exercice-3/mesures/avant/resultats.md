# Mesures : avant

Le 06/11/2026 à 13:49.

PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) ; `shared_buffers` = 2GB, `work_mem` = 128MB, `effective_cache_size` = 8GB, `max_parallel_workers_per_gather` = 4.

## Volume

| Table | Lignes | Données (Mo) | Index (Mo) | Total (Mo) | Octets / ligne |
|:--|--:|--:|--:|--:|--:|
| `dim_temps` | 11 323 | 0,5 | 0,3 | 0,8 | 72 |
| `dim_territoire` | 101 | 0,0 | 0,0 | 0,1 | 568 |
| `dim_forme_juridique` | 1 792 | 0,2 | 0,2 | 0,4 | 210 |
| `dim_entreprise` | 9 239 056 | 363,4 | 197,9 | 561,3 | 64 |
| `fait_annonce` | 10 917 086 | 577,2 | 452,7 | 1 029,9 | 99 |
| `fait_bilan` | 6 336 013 | 374,7 | 135,8 | 510,5 | 84 |
| **Total** |  | **1 316,0** | **786,8** | **2 102,8** |  |

Base `dw` entière : 2 110,4 Mo.

### Index

| Index | Table | Taille (Mo) |
|:--|:--|--:|
| `fait_annonce_pkey` | `fait_annonce` | 233,9 |
| `fait_annonce_siren_idx` | `fait_annonce` | 218,7 |
| `dim_entreprise_pkey` | `dim_entreprise` | 197,9 |
| `fait_bilan_pkey` | `fait_bilan` | 135,7 |
| `dim_temps_pkey` | `dim_temps` | 0,3 |
| `dim_forme_juridique_libelle_key` | `dim_forme_juridique` | 0,1 |
| `dim_forme_juridique_pkey` | `dim_forme_juridique` | 0,1 |
| `fait_annonce_jour_idx` | `fait_annonce` | 0,0 |
| `fait_bilan_date_cloture_idx` | `fait_bilan` | 0,0 |
| `dim_territoire_pkey` | `dim_territoire` | 0,0 |

## Temps des requêtes

Médiane de 5 exécutions après une exécution de chauffe (cache chaud), temps mesuré par `EXPLAIN ANALYZE`. Plans détaillés dans `plans/`.

| Requête | Rôle | Médiane (ms) | Min (ms) | Données lues (Mo) |
|:--|:--|--:|--:|--:|
| `creations_radiations` | Notebook §3 et site : sociétés créées / radiées par année | 2 044 | 1 757 | 943,6 |
| `is_par_tranche` | Notebook §4 et rapport : IS par tranche de CA (2023) | 292 | 286 | 379,6 |
| `indicateur_controle` | DAG chargement_dw, tâche controler : créations / radiations hors office | 2 019 | 1 832 | 943,6 |
| `creations_departement_2023` | Une période : créations par département au 1er trimestre 2023 | 23,8 | 23,7 | 11,3 |
| `historique_entreprise` | Une entreprise : annonces et bilans d'un SIREN | 0,04 | 0,03 | 0,1 |

## Durée du chargement (dernier `chargement_dw` réussi)

Exécution `manual__2026-11-06T12:42:11.105731+00:00`.

| Tâche | Durée (s) |
|:--|--:|
| `verifier_sources` | 0 |
| `nettoyer_annonces` | 13 |
| `nettoyer_bilans` | 13 |
| `creer_schema` | 0 |
| `charger_dimensions` | 4 |
| `charger_fait_bilan` | 6 |
| `charger_fait_annonce` | 8 |
| `indexer` | 27 |
| `controler` | 5 |
| **Total du DAG** | **62** |
