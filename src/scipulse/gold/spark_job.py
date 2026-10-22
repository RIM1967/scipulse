"""Job Spark Gold : Silver × 3 (MinIO) → `arxiv-papers-enriched` (Elasticsearch).

Lancé par Airflow via SparkSubmitOperator :
    spark-submit --master spark://spark-master:7077 .../spark_job.py

Étapes :
  1. lecture des trois couches Silver (Parquet, s3a://) ;
  2. jointure à 3 sources + taux de correspondance + comparaison des stratégies ;
  3. window functions : croissance par catégorie, vélocité, centiles ;
  4. TF-IDF (mots-clés) puis option C (KMeans + PCA) ;
  5. option B : PageRank sur le graphe de citations ;
  6. score d'impact composite + segments ;
  7. écriture Gold via elasticsearch-hadoop (seule écriture vers ES du Module 2).
Un rapport JSON (couverture, plans, clustering, graphe) est écrit dans data/reports/.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from scipulse.config import Bucket, EsIndex, Settings
from scipulse.gold import joins, scoring, text
from scipulse.gold.es_index import recreate_index
from scipulse.gold.graph import citation_graph
from scipulse.gold.spark import build_session
from scipulse.monitoring import Layer, MetricsRecorder

log = logging.getLogger("scipulse.gold")

# Colonnes publiées dans l'index Gold (doit correspondre au mapping strict)
GOLD_COLUMNS = [
    "arxiv_id", "title", "abstract", "authors", "n_authors", "categories",
    "primary_category", "primary_domain", "category_name", "published_at",
    "published_year", "journal_doi", "arxiv_doi",
    "openalex_id", "matched_doi", "cited_by_count", "fwci", "topic", "topic_subfield",
    "topic_field", "topic_domain", "citations_last_2y", "citation_velocity",
    "citations_per_year", "citation_pct_in_year", "citation_pct_in_category",
    "has_citations", "hn_stories", "hn_points", "hn_max_points", "hn_comments",
    "hn_first_seen_at", "hn_top_title", "hn_top_story_id", "hn_pct", "on_hn",
    "category_year_papers", "category_yoy_growth_pct", "keywords", "cluster_id",
    "cluster_label", "x_pca", "y_pca", "pagerank", "pagerank_pct", "component_id",
    "hn_signal", "recency_signal", "impact_score", "impact_segment", "enriched_at",
]


def s3(bucket: str, prefix: str) -> str:
    return f"s3a://{bucket}/{prefix}/"


def add_pagerank(gold: DataFrame, report: dict) -> tuple[DataFrame, bool]:
    """Option B, tolérante aux pannes : sans graphe exploitable, le score s'en passe."""
    try:
        graph_df, graph_report = citation_graph(gold)
        report["graph"] = asdict(graph_report)
        if graph_report.edges == 0:
            raise ValueError("aucune arête dans le sous-graphe du corpus")
        ranked = graph_df.withColumn(
            "pagerank_pct", F.round(F.percent_rank().over(Window.orderBy("pagerank")), 4))
        return gold.join(ranked, "arxiv_id", "left"), True
    except Exception as exc:  # noqa: BLE001 — l'option B ne doit jamais bloquer le Gold
        log.warning("PageRank ignoré : %s", exc)
        report["graph"] = {"skipped": str(exc)[:300]}
        return (gold.withColumn("pagerank", F.lit(None).cast("double"))
                    .withColumn("pagerank_pct", F.lit(None).cast("double"))
                    .withColumn("component_id", F.lit(None).cast("long"))), False


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    recorder = MetricsRecorder(settings.metrics_dir)
    current_year = datetime.now(UTC).year
    report: dict = {"run_at": datetime.now(UTC).isoformat()}

    with recorder.track(pipeline="gold", layer=Layer.GOLD, source="spark",
                        task="spark_gold_enrichment") as metrics:
        spark = build_session(settings)

        # 1. Silver
        arxiv = spark.read.parquet(s3(Bucket.ARXIV_CLEAN, "arxiv"))
        hn = spark.read.parquet(s3(Bucket.HN_CLEAN, "hn"))
        citations = spark.read.parquet(s3(Bucket.CITATIONS_CLEAN, "openalex"))

        # 2. Jointures
        cites = joins.citations_per_paper(arxiv, citations)
        hn_agg = joins.hn_per_paper(hn).cache()
        report["join_strategies"] = joins.compare_join_strategies(spark, arxiv, hn_agg)
        gold = joins.join_sources(arxiv, cites, hn_agg).cache()
        coverage = joins.match_coverage(gold, hn).as_dict()
        report["coverage"] = coverage
        log.info("Couverture : %s", coverage)

        # 3. Window functions
        gold = gold.join(joins.category_growth(arxiv),
                         ["primary_category", "published_year"], "left")
        gold = joins.percentiles(joins.citation_velocity(gold, current_year)).cache()

        # 4. Texte : TF-IDF puis clustering (option C)
        gold, _ = text.tfidf_keywords(text.with_text(gold))
        gold, clusters = text.topic_clusters(gold)
        report["clustering"] = asdict(clusters)

        # 5. Graphe (option B)
        gold, with_pagerank = add_pagerank(gold, report)

        # 6. Score
        gold = scoring.add_segments(
            scoring.add_impact_score(gold, current_year, with_pagerank=with_pagerank))
        gold = gold.withColumn("enriched_at", F.current_timestamp()).select(*GOLD_COLUMNS)

        # 7. Écriture Gold
        from elasticsearch import Elasticsearch

        recreate_index(Elasticsearch(settings.es_host, request_timeout=120),
                       EsIndex.ARXIV_ENRICHED)
        (gold.write.format("org.elasticsearch.spark.sql")
             .option("es.resource", EsIndex.ARXIV_ENRICHED.value)
             .option("es.mapping.id", "arxiv_id")
             .option("es.batch.size.entries", "2000")
             .mode("append")
             .save())

        written = spark.read.format("org.elasticsearch.spark.sql") \
            .load(EsIndex.ARXIV_ENRICHED.value).count()
        metrics.records_in = coverage["arxiv_papers"]
        metrics.records_out = written
        metrics.extra = {"coverage": coverage, "best_k": clusters.k,
                         "pagerank": with_pagerank}
        spark.stop()

    reports_dir = settings.data_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "gold_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
