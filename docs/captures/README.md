# Captures d'écran

Preuves visuelles du fonctionnement du pipeline (référencées dans `docs/validation.md`).

| Fichier | Contenu |
|---|---|
| `01_airflow_dags.png` | Les 3 DAGs Airflow (poller HN exécuté toutes les 5 min) |
| `02_minio_buckets.png` | Buckets Medallion dans MinIO, Silver en Parquet |
| `03_gx_data_docs.png` | Data Docs Great Expectations : validations réussies |
| `04_kibana_exploration_1.png` | Exploration ArXiv : indicateurs, distribution des catégories |
| `05_kibana_exploration_2.png` | Exploration ArXiv : évolution annuelle par catégorie |
| `06_kibana_exploration_3.png` | Exploration ArXiv : article le plus récent par domaine, articles récents |
| `07_kibana_monitoring_1.png` | Monitoring : exécutions, échecs, taux de succès GX |
| `08_kibana_monitoring_2.png` | Monitoring : volume écrit par couche (Bronze / Silver / Gold) |
| `09_airflow_pipeline_vert.png` | DAG `scipulse_arxiv_pipeline` entièrement réussi (Bronze → Silver → Gold) |
| `10_kibana_insights_1.png` | Insights : indicateurs, top score d'impact, citations × Hacker News |
| `11_kibana_insights_2.png` | Insights : très cités jamais sur HN / viraux sur HN peu cités |
| `12_kibana_insights_3.png` | Insights : sous-domaines en croissance, segments, mots-clés TF-IDF |
| `13_kibana_insights_4.png` | Insights : carte thématique KMeans + PCA, PageRank |
| `14_spark_ui.png` | Interface du master Spark avec le job terminé |
| `15_github_ci.png` | CI GitHub Actions verte |

Les 7 « exécutions en échec » du monitoring sont les incidents réels de mise
au point (corrigés, voir *Problèmes rencontrés* dans le README) : le
dashboard les conserve volontairement comme historique.
