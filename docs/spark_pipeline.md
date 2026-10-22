# Pipeline Spark — SciPulse Insights (Module 2)

Job : [`src/scipulse/gold/spark_job.py`](../src/scipulse/gold/spark_job.py), lancé par
la tâche Airflow `spark_gold_enrichment` (`SparkSubmitOperator`) sur le cluster
standalone (1 master, 1 worker à 2 cœurs / 2 Go). Le driver tourne dans le
conteneur Airflow (mode client).

Chaque exécution produit un rapport complet : `data/reports/gold_report.json`
(couverture, plans d'exécution, clustering, graphe).

```
Silver (MinIO, s3a://)            Spark                                        Gold (ES)
──────────────────────            ─────                                        ─────────
arxiv-clean/     100 000  ─┐
citations-clean/  96 511  ─┼─► jointure ×3 ─► windows ─► TF-IDF ─► KMeans/PCA ─► PageRank ─► score ─► arxiv-papers-enriched
hn-clean/         24 270  ─┘
```

---

## 1. Choix de la source de citations

**OpenAlex** — justification complète dans le [README](../README.md#source-de-citations--openalex-plutôt-que-semantic-scholar) :
requêtes par lots de 100 DOI, `counts_by_year` (vélocité), `referenced_works`
(graphe), `primary_topic` (champ disciplinaire), licence CC0.

## 2. Jointure à trois sources

Granularité cible : **une ligne par article ArXiv** (jointure gauche : aucun
article n'est perdu, l'enrichissement est facultatif).

### ArXiv × OpenAlex — deux clés possibles

Un article peut être connu d'OpenAlex sous son **DOI ArXiv**
(`10.48550/arxiv.<id>`) ou sous le **DOI de sa version publiée** en revue.
Une condition `doi = arxiv_doi OR doi = journal_doi` empêcherait toute
optimisation (Spark se rabat sur un *nested loop join*). On **explose** donc
les deux DOI en lignes, puis on fait une **equi-join** sur `doi` :

```python
paper_dois = arxiv.select("arxiv_id",
    F.explode(F.array_compact(F.array("arxiv_doi", "journal_doi"))).alias("doi"))
best = Window.partitionBy("arxiv_id").orderBy(F.desc("cited_by_count"))
paper_dois.join(citations, "doi").withColumn("_rank", F.row_number().over(best)).where("_rank = 1")
```

Si les deux DOI trouvent un work, on garde le plus cité (fenêtre `row_number`).

### ArXiv × Hacker News — agréger avant de joindre

Un article peut avoir plusieurs stories HN. Les agréger **avant** la jointure
(nombre de stories, somme et max des points, commentaires, première date)
évite de dupliquer les lignes ArXiv.

### Taux de correspondance (match coverage)

| Indicateur | Valeur | % |
|---|---:|---:|
| Articles ArXiv (échantillon) | 100 000 | — |
| … avec citations OpenAlex | 61 269 | **61,3 %** |
| … mentionnés sur Hacker News | 1 702 | 1,7 % |
| … avec les deux | 1 203 | 1,2 % |
| Stories HN pointant vers arxiv.org | 23 060 | — |
| … dont l'article est dans l'échantillon | 2 132 | 9,2 % |

**Interprétation**
- **61 % côté citations** : les 39 % restants sont surtout des preprints
  récents (pas encore indexés ou fusionnés par OpenAlex) et des articles dont
  le DOI ArXiv n'est pas encore rattaché au work. La double clé (DOI ArXiv +
  DOI éditeur) et le nettoyage des DOI en Silver améliorent sensiblement ce taux.
- **9 % côté HN** : attendu. L'échantillon couvre 100 000 des ~600 000
  articles d'informatique depuis 2015, et une partie des stories HN pointent
  vers d'autres disciplines (physique, mathématiques, biologie).
- **1,7 % des articles sur HN** : la popularité grand public est un signal
  **rare** — d'où le log-scaling et le segment `viral_only`.

## 3. Broadcast join vs sort-merge join

Comparaison forcée (AQE et auto-broadcast désactivés pour isoler chaque
stratégie) sur `arxiv (100 000) ⟕ hn_agg (~1 700)`, plans lus avec
`explain(mode="formatted")`. Plans complets et temps dans `gold_report.json`.

| | Broadcast hash join | Sort-merge join |
|---|---|---|
| Plan physique | `BroadcastHashJoin` + `BroadcastExchange` sur `hn_agg` | `SortMergeJoin` + `Exchange hashpartitioning` **et** `Sort` des **deux** côtés |
| Shuffle de la grande table | **aucun** | oui (100 000 lignes redistribuées) |
| Condition | petite table < mémoire de chaque executor | toujours applicable |

**Mesures** (run du 22/10/2026, cluster 1 worker × 2 cœurs) :

| Stratégie | Temps mesuré | Opérateurs du plan |
|---|---:|---|
| Broadcast hash join | 19,9 s | `BroadcastHashJoin LeftOuter BuildRight` ← `BroadcastExchange` |
| Sort-merge join | 6,3 s | `SortMergeJoin LeftOuter` ← `Sort` ← `Exchange` sur les deux côtés |

**Lecture critique des temps** : la variante broadcast est exécutée **en
premier** ; son temps inclut la première lecture des Parquet depuis MinIO
(listing S3, ouverture des connexions, chauffe de la JVM), dont la seconde
mesure profite ensuite. À 100 000 lignes, le coût du shuffle est de toute
façon négligeable devant ces coûts fixes : **les temps ne départagent pas
les stratégies à cette échelle**, ce sont les **plans** qui le font. Le plan
sort-merge montre un `Exchange` + `Sort` sur les 100 000 lignes ArXiv ; le
plan broadcast n'en a aucun sur la grande table — c'est cet écart qui
grandit linéairement avec le volume (millions d'articles).

**Choix retenu : broadcast** pour l'agrégat HN (quelques milliers de lignes,
quelques centaines de Ko) : on évite le shuffle et le tri de la table ArXiv.
Le sort-merge reste la bonne stratégie pour joindre deux grandes tables
(ArXiv × citations, ~100 000 × ~100 000), que Spark choisit de lui-même ;
en production, l'**AQE** (activé dans le job) convertit dynamiquement un
sort-merge en broadcast quand une table se révèle petite après filtrage.

## 4. Indicateurs (window functions)

| Indicateur | Fenêtre | Définition |
|---|---|---|
| `category_yoy_growth_pct` | `partitionBy(primary_category).orderBy(published_year)` + `lag` | Croissance annuelle des publications de la catégorie |
| `citations_last_2y`, `citation_velocity` | fonctions d'ordre supérieur sur `counts_by_year` | Citations des 2 dernières années, et leur moyenne annuelle |
| `citations_per_year` | — | Citations totales / âge de l'article |
| `citation_pct_in_year` | `partitionBy(published_year)` + `percent_rank` | Centile de citations parmi les articles de la même année |
| `citation_pct_in_category` | `partitionBy(primary_category)` + `percent_rank` | Centile au sein de la catégorie |
| `hn_pct` | `partitionBy(on_hn)` + `percent_rank` | Centile du signal HN parmi les articles présents sur HN |

## 5. TF-IDF distribué (MLlib)

`RegexTokenizer` → `StopWordsRemover` (anglais + mots vides scientifiques :
*propose, approach, results*…) → `CountVectorizer` (20 000 termes, `minDF=5`,
`maxDF=0.5`) → `IDF`.

`CountVectorizer` plutôt que `HashingTF` : il conserve le **vocabulaire**,
indispensable pour restituer des mots-clés lisibles. Les 8 termes de plus fort
poids TF-IDF de chaque article sont publiés dans le champ `keywords` (nuage de
mots du dashboard Insights).

## 6. Score d'impact composite

```
impact = 100 × ( 0.45 · C  +  0.25 · H  +  0.15 · R  +  0.15 · P )
```

Définitions et justification des pondérations : [README — Score d'impact](../README.md#score-dimpact).
Implémentation Spark et référence Python testée unitairement :
[`gold/scoring.py`](../src/scipulse/gold/scoring.py).

Choix clés :
- **centile par cohorte** plutôt que citations brutes : un article de 2024 n'a
  pas eu le temps d'accumuler les citations d'un article de 2016 ;
- **log-scaling HN** : la distribution des points HN est très asymétrique ;
- **demi-vie de 2 ans** pour la récence ;
- **redistribution** des poids si le graphe de citations est inexploitable.

## 7. Option C — clustering thématique (KMeans + PCA)

| Hyperparamètre | Valeur | Justification |
|---|---|---|
| Vocabulaire | 2 000 termes | La PCA MLlib calcule une matrice de covariance d×d sur le driver : 2 000² ≈ 32 Mo (contre 3,2 Go pour 20 000) |
| Normalisation | L2 | Les abstracts ont des longueurs variables |
| Distance | cosinus | Adaptée aux vecteurs TF-IDF creux |
| k | **choisi parmi {8, 12, 16, 20} par silhouette** (cosinus, échantillon de 20 000) | Critère interne, sans étiquette |
| PCA | 2 composantes | Carte 2D `x_pca` / `y_pca` dans Kibana |

Chaque cluster est **libellé** par les 4 termes de plus fort poids de son
centre, et comparé aux catégories ArXiv déclarées (part de la catégorie
dominante dans le cluster, dans `gold_report.json → clustering`).

**Résultats**

| k | 8 | 12 | 16 | **20** |
|---|---|---|---|---|
| Silhouette (cosinus) | 0,015 | 0,017 | 0,018 | **0,024** |

k = 20 est retenu. Les silhouettes sont faibles en valeur absolue, ce qui est
**normal pour du texte** (vecteurs creux de haute dimension, thématiques qui
se recouvrent) ; c'est la comparaison relative entre k qui guide le choix.
La silhouette croît encore à k = 20 (borne de la grille) : une grille plus
large est une piste d'amélioration.

| Cluster (termes du centre) | Catégorie ArXiv dominante | Part |
|---|---|---:|
| video, object, motion, detection | cs.CV | 77 % |
| segmentation, semantic, image | cs.CV | 71 % |
| image, images, diffusion, resolution | cs.CV | 70 % |
| quantum, classical, circuits | quant-ph | 62 % |
| language, text, models, translation | cs.CL | 61 % |
| control, robot, robots, planning | cs.RO | 54 % |
| llms, reasoning, llm, language | cs.CL | 44 % |
| attacks, adversarial, attack, security | cs.CR | 37 % |
| privacy, federated, data, clients | cs.LG | 38 % |
| agent, agents, policy, reinforcement | cs.LG | 25 % |

**Interprétation** : les clusters « vision » et « quantique » recoupent
fortement les catégories déclarées ; d'autres sont **transverses** (agents,
apprentissage fédéré, LLM) et regroupent des articles de plusieurs
catégories — exactement ce que la catégorisation manuelle d'ArXiv ne montre
pas. La PCA à 2 composantes n'explique qu'une faible part de variance
(≈ 1,4 %) : la carte sert à **visualiser** les voisinages, pas à résumer
l'information.

## 8. Option B — graphe de citations (GraphFrames)

- **Sommets** : articles du corpus ayant un work OpenAlex.
- **Arêtes** : citant → cité, issues de `referenced_works`, restreintes au
  corpus (sous-graphe induit).
- **PageRank** (`resetProbability = 0.15`, 10 itérations) → `pagerank`, `pagerank_pct`.
- **Composantes connexes** (algorithme GraphX, sans répertoire de checkpoint partagé).

Le PageRank mesure l'influence **structurelle** : être cité par des articles
eux-mêmes influents compte davantage qu'un simple comptage. L'échantillon
étant aléatoire (100 000 articles), le sous-graphe est clairsemé : la
composante PageRank reste un complément (15 %) et non le critère principal.
L'option est tolérante aux pannes : sans arête exploitable, le score
redistribue son poids.

**Résultats** : 61 269 sommets, **13 670 arêtes** de citation internes au
corpus, 51 509 composantes connexes (majoritairement des articles isolés),
dont une **composante géante de 7 260 articles**. Articles les plus
influents : *Show, Attend and Tell*, *Decoupled Weight Decay Regularization*,
*Large-Margin Softmax*, *Understanding deep learning requires rethinking
generalization*, *InfoGAN*.

## Résultats analytiques

| Question | Résultat |
|---|---|
| Score d'impact | min 0,3 — moyenne 18,6 — max 94,7 |
| Top impact | *DeepSeek-R1* (94,7 : 1 127 citations, 1 351 points HN), *Mixtral of Experts*, *Diffusion Models Beat GANs* |
| Très cités, jamais sur HN | **8 295** articles — *nnU-Net* (10 368 citations), *Contrastive Predictive Coding*, *Matching Networks* |
| Viraux sur HN, peu cités | **65** articles — *Pen and Paper Exercises in ML* (784 points, 0 citation), *σ-GPTs* |
| Sous-domaines en croissance (depuis 2020) | `cs.AI` : plus forte croissance (~37 %/an) ; `cs.HC`, `cs.CY`, `quant-ph` : meilleur impact moyen ; `cs.LG`, `cs.CV` : plus gros volumes |

## 9. Écriture Gold

```python
gold.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "arxiv-papers-enriched") \
    .option("es.mapping.id", "arxiv_id") \
    .mode("append").save()
```

- L'index est **recréé** avec son mapping versionné
  ([`elasticsearch/arxiv-papers-enriched.json`](../elasticsearch/arxiv-papers-enriched.json),
  `dynamic: strict`) juste avant l'écriture : les types ne sont jamais devinés.
- `es.mapping.id = arxiv_id` : réécriture **idempotente** (un article = un document).
- `es.nodes.wan.only = true` : pas de découverte des nœuds (réseau Docker).

## 10. Orchestration

`[clean_citations_silver, clean_hn_silver] >> spark_gold_enrichment >> validate_gold`

`validate_gold` ([`gold/validation.py`](../src/scipulse/gold/validation.py)) contrôle
l'index (non vide, score dans [0, 100], citations et HN présents) et exécute
les **requêtes analytiques cibles** ; le DAG n'est vert que si elles
renvoient des résultats.
