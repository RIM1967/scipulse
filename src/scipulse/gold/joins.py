"""Jointure à trois sources (ArXiv × OpenAlex × Hacker News) et analyse de couverture.

Granularité cible : **une ligne par article ArXiv** (table de faits Gold).
  - OpenAlex : un article peut correspondre via son DOI ArXiv *ou* son DOI
    éditeur. On « explose » les deux DOI en lignes (equi-join, donc
    optimisable) au lieu d'une condition OR (qui forcerait un nested loop),
    puis on garde le work le plus cité par article ;
  - Hacker News : plusieurs stories par article → agrégées avant jointure
    (évite la duplication des lignes ArXiv).
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def citations_per_paper(arxiv: DataFrame, citations: DataFrame) -> DataFrame:
    """Un work OpenAlex (le plus cité) par article, via DOI ArXiv ou DOI éditeur."""
    paper_dois = arxiv.select(
        "arxiv_id",
        F.explode(F.array_compact(F.array("arxiv_doi", "journal_doi"))).alias("doi"),
    )
    best = Window.partitionBy("arxiv_id").orderBy(F.desc("cited_by_count"))
    return (paper_dois.join(citations.drop("arxiv_id"), "doi")
            .withColumn("_rank", F.row_number().over(best))
            .where("_rank = 1")
            .drop("_rank")
            .withColumnRenamed("doi", "matched_doi"))


def hn_per_paper(hn: DataFrame) -> DataFrame:
    """Signal Hacker News agrégé par article."""
    return (hn.where(F.col("arxiv_id").isNotNull())
            .groupBy("arxiv_id")
            .agg(F.count("*").alias("hn_stories"),
                 F.sum("score").alias("hn_points"),
                 F.max("score").alias("hn_max_points"),
                 F.sum("num_comments").alias("hn_comments"),
                 F.min("created_at").alias("hn_first_seen_at"),
                 F.max_by("title", "score").alias("hn_top_title"),
                 F.max_by("hn_id", "score").alias("hn_top_story_id")))


def join_sources(arxiv: DataFrame, cites: DataFrame, hn_agg: DataFrame,
                 broadcast_small: bool = True) -> DataFrame:
    """Jointure gauche : tous les articles sont conservés, enrichis si possible.

    `hn_agg` (quelques milliers de lignes) est diffusé à tous les executors
    (broadcast hash join) : pas de shuffle de la grande table ArXiv.
    """
    hn_side = F.broadcast(hn_agg) if broadcast_small else hn_agg
    return (arxiv.join(cites, "arxiv_id", "left")
                 .join(hn_side, "arxiv_id", "left")
                 .fillna({"hn_stories": 0, "hn_points": 0, "hn_max_points": 0,
                          "hn_comments": 0})
                 .withColumn("has_citations", F.col("openalex_id").isNotNull())
                 .withColumn("on_hn", F.col("hn_stories") > 0))


@dataclass
class Coverage:
    arxiv_papers: int
    with_citations: int
    on_hn: int
    with_both: int
    hn_arxiv_stories: int
    hn_stories_matched: int

    def as_dict(self) -> dict[str, float | int]:
        d: dict[str, float | int] = asdict(self)
        n = max(self.arxiv_papers, 1)
        d["citations_coverage_pct"] = round(100 * self.with_citations / n, 2)
        d["hn_coverage_pct"] = round(100 * self.on_hn / n, 2)
        d["both_coverage_pct"] = round(100 * self.with_both / n, 2)
        d["hn_side_match_pct"] = round(
            100 * self.hn_stories_matched / max(self.hn_arxiv_stories, 1), 2)
        return d


def match_coverage(gold: DataFrame, hn: DataFrame) -> Coverage:
    """Taux de correspondance, vu côté ArXiv et côté Hacker News."""
    row = gold.agg(
        F.count("*").alias("n"),
        F.sum(F.col("has_citations").cast("int")).alias("cit"),
        F.sum(F.col("on_hn").cast("int")).alias("hn"),
        F.sum((F.col("has_citations") & F.col("on_hn")).cast("int")).alias("both"),
    ).first()
    hn_linked = hn.where(F.col("arxiv_id").isNotNull())
    matched = hn_linked.join(gold.select("arxiv_id"), "arxiv_id", "left_semi").count()
    return Coverage(row["n"], row["cit"] or 0, row["hn"] or 0, row["both"] or 0,
                    hn_linked.count(), matched)


def compare_join_strategies(spark: SparkSession, arxiv: DataFrame, hn_agg: DataFrame
                            ) -> dict[str, dict[str, str | float]]:
    """Broadcast hash join vs sort-merge join : plan physique + temps mesuré.

    On désactive AQE et le seuil d'auto-broadcast pour forcer chaque stratégie
    et lire le plan réellement choisi (`explain(mode="formatted")`).
    """
    saved = {k: spark.conf.get(k) for k in
             ("spark.sql.adaptive.enabled", "spark.sql.autoBroadcastJoinThreshold")}
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    variants = {
        "broadcast_hash_join": arxiv.join(F.broadcast(hn_agg), "arxiv_id", "left"),
        "sort_merge_join": arxiv.join(hn_agg, "arxiv_id", "left"),
    }
    report: dict[str, dict[str, str | float]] = {}
    try:
        for name, df in variants.items():
            # Même appel que DataFrame.explain(), mais en récupérant le texte
            plan = spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(),
                                                           "formatted")
            start = time.perf_counter()
            df.write.format("noop").mode("overwrite").save()   # exécution complète, sans écriture
            report[name] = {"seconds": round(time.perf_counter() - start, 2), "plan": plan}
    finally:
        for key, value in saved.items():
            spark.conf.set(key, value)
    return report


# ── Window functions ─────────────────────────────────────────────────────────

def category_growth(arxiv: DataFrame) -> DataFrame:
    """Croissance annuelle des publications par catégorie (lag sur l'année)."""
    by_year = Window.partitionBy("primary_category").orderBy("published_year")
    return (arxiv.groupBy("primary_category", "published_year")
            .agg(F.count("*").alias("category_year_papers"))
            .withColumn("_prev", F.lag("category_year_papers").over(by_year))
            .withColumn("category_yoy_growth_pct",
                        F.round(100 * (F.col("category_year_papers") - F.col("_prev"))
                                / F.col("_prev"), 2))
            .drop("_prev"))


def citation_velocity(df: DataFrame, current_year: int) -> DataFrame:
    """Vélocité de citation = citations des 2 dernières années / 2,
    et citations par année depuis la publication."""
    recent = F.filter("counts_by_year", lambda c: c["year"] >= current_year - 1)
    age = F.greatest(F.lit(1), F.lit(current_year) - F.col("published_year") + 1)
    return (df.withColumn("citations_last_2y",
                          # Accumulateur en long : même type que cited_by_count (Parquet int64)
                          F.coalesce(F.aggregate(recent, F.lit(0).cast("long"),
                                                 lambda acc, c: acc + c["cited_by_count"]),
                                     F.lit(0).cast("long")))
              .withColumn("citation_velocity", F.col("citations_last_2y") / 2.0)
              .withColumn("citations_per_year",
                          F.round(F.coalesce("cited_by_count", F.lit(0)) / age, 3)))


def percentiles(df: DataFrame) -> DataFrame:
    """Rangs centiles : citations au sein de la même cohorte (année de publication),
    pour ne pas pénaliser les articles récents ; signal HN parmi les articles sur HN."""
    cohort = Window.partitionBy("published_year").orderBy(F.coalesce("cited_by_count", F.lit(0)))
    in_category = Window.partitionBy("primary_category").orderBy(
        F.coalesce("cited_by_count", F.lit(0)))
    hn_window = Window.partitionBy("on_hn").orderBy("hn_points")
    return (df.withColumn("citation_pct_in_year", F.round(F.percent_rank().over(cohort), 4))
              .withColumn("citation_pct_in_category",
                          F.round(F.percent_rank().over(in_category), 4))
              .withColumn("hn_pct", F.when(F.col("on_hn"),
                                           F.round(F.percent_rank().over(hn_window), 4))
                                     .otherwise(F.lit(0.0))))
