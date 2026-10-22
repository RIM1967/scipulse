# Procédure de validation

Objectif : démontrer que (1) la stack démarre de zéro, (2) le pipeline
**généralise** à un autre jeu de données que celui de développement, et
(3) les dashboards répondent aux **requêtes analytiques cibles**.

---

## 1. Reproductibilité de l'infrastructure

```bash
make clean   # supprime conteneurs ET volumes : départ de zéro
make up      # construit, démarre, attend que tout soit sain
```

Critère : `check_stack.py` affiche **« Stack opérationnelle. »** (8 lignes `OK`).

## 2. Tests automatisés

```bash
python -m pytest     # tests unitaires : S3 simulé (moto), API simulées
python -m ruff check .
```

Couvrent : normalisation des clés de jointure (dont les anomalies réelles de
DOI), nettoyage des trois sources, déduplication des snapshots HN, formule du
score (bornes, demi-vie, log-scaling, redistribution des poids), porte
qualité, alertes, cohérence des dashboards générés et du mapping Gold avec
les colonnes Spark. Exécutés par la CI à chaque push.

## 3. Généralisation : exécution sur un jeu de test indépendant

Le pipeline n'a été mis au point que sur l'échantillon de développement
(`ARXIV_SEED=42`, 100 000 articles). On le rejoue sur un **échantillon
disjoint en grande partie** (autre graine) et **plus petit**, sans modifier
une ligne de code :

```ini
# .env
ARXIV_SAMPLE_SIZE=20000
ARXIV_SEED=2026
```

```bash
docker compose up -d                       # recharge la configuration
# Airflow : déclencher scipulse_arxiv_pipeline
```

Critères de réussite (tâche `validate_gold` verte + valeurs attendues) :

| Contrôle | Attendu | Pourquoi |
|---|---|---|
| Suites GX ArXiv, HN, citations | 100 % | Les règles qualité ne sont pas sur-ajustées au jeu de dev |
| Couverture citations | même ordre de grandeur (≈ 55–65 %) | La stratégie de jointure ne dépend pas de l'échantillon |
| Couverture HN | ≈ 1,5–2 % des articles | Signal rare, proportion stable |
| Score d'impact | dans [0, 100], distribution comparable | Formule à base de centiles : invariante à la taille du corpus |
| Requêtes analytiques | non vides | Les cas d'usage existent sur tout échantillon |
| k (KMeans) | proche du jeu de dev | Structure thématique stable |

Revenir ensuite au jeu de développement (`ARXIV_SAMPLE_SIZE=100000`,
`ARXIV_SEED=42`) et relancer le DAG.

**Robustesse testée en conditions réelles** (pendant le développement) :
DOI malformés dans la source (isolés sans faire échouer le lot), quota d'API
dépassé (retry/backoff), transferts réseau coupés (retry), échec d'une suite
GX (Silver non écrasé, alerte, aval bloqué).

## 4. Requêtes analytiques cibles

Exécutées par `validate_gold` à chaque run, et à la demande :

```bash
docker exec scipulse-airflow-sched python -m scipulse.gold.validation
```

| Question analytique | Requête | Dashboard |
|---|---|---|
| Articles très cités académiquement mais jamais mentionnés sur Hacker News | `impact_segment : "academic_only"` trié par `cited_by_count` | Insights → « Très cités académiquement, jamais sur Hacker News » |
| Articles viraux sur Hacker News mais peu cités | `impact_segment : "viral_only"` trié par `hn_points` | Insights → « Viraux sur Hacker News, peu cités » |
| Sous-domaines en croissance rapide selon les deux métriques | agrégation par `primary_category` depuis 2020 : croissance annuelle moyenne × score d'impact moyen × stories HN | Insights → « Sous-domaines en croissance » |
| Articles les plus significatifs | tri par `impact_score` | Insights → « Top articles par score d'impact » |
| Distribution des catégories, évolution temporelle, articles récents | agrégations sur `arxiv-papers` | Exploration ArXiv |
| Santé du pipeline | volumes par couche, taux GX, fraîcheur, alertes | Monitoring |

Recherche plein texte (analyseur anglais avec racinisation) — exemple dans la
barre KQL du dashboard Exploration : `abstract : "large language model"`.

## 5. Captures d'écran

Voir [captures/README.md](captures/README.md) pour la liste ; fichiers dans
[`docs/captures/`](captures/).

| # | Capture | Ce qu'elle prouve |
|---|---|---|
| 01 | DAGs Airflow | Orchestration : 3 DAGs actifs, poller toutes les 5 min |
| 02 | Buckets MinIO | Medallion Bronze / Silver, Parquet |
| 03 | Data Docs GX | Validations qualité tracées et réussies |
| 04–06 | Dashboard Exploration | Module 1 : exploration ArXiv |
| 07–08 | Dashboard Monitoring | Volumes par couche, taux GX, échecs |
| 09 | DAG entièrement vert | Pipeline Bronze → Silver → Gold de bout en bout |
| 10–13 | Dashboard Insights | Module 2 : score d'impact, cas d'usage, carte thématique, PageRank |
| 14 | Spark UI | Exécution distribuée sur le cluster |
| 15 | CI GitHub | Lint + tests automatiques |
