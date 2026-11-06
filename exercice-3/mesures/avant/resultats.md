# Mesures : avant

Le 06/11/2026 à 13:05.

PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) ; `shared_buffers` = 128MB, `work_mem` = 4MB, `effective_cache_size` = 4GB, `max_parallel_workers_per_gather` = 2.

## Volume

| Table | Lignes | Données (Mo) | Index (Mo) | Total (Mo) | Octets / ligne |
|:--|--:|--:|--:|--:|--:|
| `dim_temps` | 11 323 | 0,8 | 0,3 | 1,0 | 96 |
| `dim_territoire` | 101 | 0,0 | 0,0 | 0,0 | 243 |
| `dim_entreprise` | 9 239 056 | 585,2 | 358,3 | 943,5 | 107 |
| `fait_annonce` | 10 917 086 | 749,3 | 896,2 | 1 645,5 | 158 |
| `fait_bilan` | 6 336 013 | 496,3 | 262,4 | 758,8 | 126 |
| **Total** |  | **1 831,7** | **1 517,2** | **3 348,9** |  |

Base `dw` entière : 3 356,4 Mo.

### Index

| Index | Table | Taille (Mo) |
|:--|:--|--:|
| `fait_annonce_pkey` | `fait_annonce` | 429,0 |
| `fait_annonce_siren_idx` | `fait_annonce` | 381,9 |
| `dim_entreprise_pkey` | `dim_entreprise` | 358,3 |
| `fait_bilan_pkey` | `fait_bilan` | 220,9 |
| `fait_annonce_jour_idx` | `fait_annonce` | 85,4 |
| `fait_bilan_date_cloture_idx` | `fait_bilan` | 41,6 |
| `dim_temps_pkey` | `dim_temps` | 0,3 |
| `dim_territoire_pkey` | `dim_territoire` | 0,0 |

## Temps des requêtes

Médiane de 5 exécutions après une exécution de chauffe (cache chaud), temps mesuré par `EXPLAIN ANALYZE`. Plans détaillés dans `plans/`.

| Requête | Rôle | Médiane (ms) | Min (ms) | Données lues (Mo) |
|:--|:--|--:|--:|--:|
| `creations_radiations` | Notebook §3 et site : sociétés créées / radiées par année | 2 961 | 2 746 | 1 336,9 |
| `is_par_tranche` | Notebook §4 et rapport : IS par tranche de CA (2023) | 375 | 369 | 499,2 |
| `indicateur_controle` | DAG chargement_dw, tâche controler : créations / radiations hors office | 3 185 | 2 984 | 1 336,9 |
| `creations_departement_2023` | Une période : créations par département au 1er trimestre 2023 | 30,7 | 30,2 | 16,3 |
| `historique_entreprise` | Une entreprise : annonces et bilans d'un SIREN | 0,04 | 0,04 | 0,1 |

## Durée du chargement (dernier `chargement_dw` réussi)

Exécution `manual__2026-11-06T11:53:02.378987+00:00`.

| Tâche | Durée (s) |
|:--|--:|
| `verifier_sources` | 4 |
| `nettoyer_annonces` | 12 |
| `nettoyer_bilans` | 13 |
| `charger_dimensions` | 54 |
| `charger_fait_bilan` | 99 |
| `charger_fait_annonce` | 349 |
| `controler` | 13 |
| **Total du DAG** | **436** |
