"""Score d'impact composite (0–100) et segmentation analytique.

    impact = 100 × ( w_cit · C + w_hn · H + w_rec · R + w_pr · P )

  C  — impact académique : rang centile des citations *dans la cohorte de
       l'année de publication*. Les citations s'accumulent avec le temps ;
       comparer un article de 2016 à un de 2024 en valeur brute n'a pas de
       sens. Le centile est aussi robuste aux valeurs extrêmes (loi de puissance).
  H  — signal Hacker News, log-scalé :
       log1p(points + commentaires) / log1p(max du corpus). Le log écrase
       l'effet « une story virale à 3 000 points » qui écraserait tout le reste.
  R  — récence : décroissance exponentielle de demi-vie 2 ans,
       R = 0.5 ^ (âge / 2). Un article de l'année vaut 1, de 2 ans 0.5...
  P  — option B : rang centile du PageRank dans le graphe de citations.

Pondérations (somme = 1) : la veille scientifique privilégie l'impact
académique avéré (0.45), complété par l'écho dans la communauté tech
(0.25), la fraîcheur (0.15) et l'influence structurelle (0.15). Sans
PageRank, son poids est redistribué proportionnellement.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

RECENCY_HALF_LIFE_YEARS = 2.0


@dataclass(frozen=True)
class Weights:
    citations: float = 0.45
    hn: float = 0.25
    recency: float = 0.15
    pagerank: float = 0.15

    def without_pagerank(self) -> Weights:
        total = self.citations + self.hn + self.recency
        return Weights(self.citations / total, self.hn / total, self.recency / total, 0.0)


# ── Référence en Python pur (testée unitairement, documente la formule) ──────

def recency(age_years: float) -> float:
    return 0.5 ** (max(age_years, 0.0) / RECENCY_HALF_LIFE_YEARS)


def hn_signal(points: int, comments: int, corpus_max: int) -> float:
    if corpus_max <= 0:
        return 0.0
    return math.log1p(points + comments) / math.log1p(corpus_max)


def impact(citation_pct: float, hn: float, rec: float, pagerank_pct: float | None,
           weights: Weights | None = None) -> float:
    weights = weights or Weights()
    w = weights if pagerank_pct is not None else weights.without_pagerank()
    return round(100 * (w.citations * citation_pct + w.hn * hn + w.recency * rec
                        + w.pagerank * (pagerank_pct or 0.0)), 2)


# ── Implémentation Spark (même formule, colonnes) ────────────────────────────

def add_impact_score(df: DataFrame, current_year: int, weights: Weights | None = None,
                     with_pagerank: bool = True) -> DataFrame:
    weights = weights or Weights()
    w = weights if with_pagerank else weights.without_pagerank()
    hn_raw = F.col("hn_points") + F.col("hn_comments")
    corpus_max = df.agg(F.max(hn_raw)).first()[0] or 0
    age = F.greatest(F.lit(0), F.lit(current_year) - F.col("published_year"))

    hn_col: Column = (F.log1p(hn_raw) / math.log1p(corpus_max)) if corpus_max > 0 else F.lit(0.0)
    pagerank_col: Column = F.coalesce(F.col("pagerank_pct"), F.lit(0.0)) if with_pagerank \
        else F.lit(0.0)

    return (df.withColumn("hn_signal", F.round(hn_col, 4))
              .withColumn("recency_signal",
                          F.round(F.pow(F.lit(0.5), age / RECENCY_HALF_LIFE_YEARS), 4))
              .withColumn("impact_score", F.round(100 * (
                  w.citations * F.col("citation_pct_in_year")
                  + w.hn * F.col("hn_signal")
                  + w.recency * F.col("recency_signal")
                  + w.pagerank * pagerank_col), 2)))


def add_segments(df: DataFrame) -> DataFrame:
    """Segments des cas d'usage du dashboard augmenté.

      academic_only : top 10 % des citations de sa cohorte, jamais sur HN ;
      viral_only    : top 25 % des articles présents sur HN, citations
                      *connues* (work OpenAlex trouvé) sous la médiane de sa
                      cohorte — un article absent d'OpenAlex n'est pas « peu
                      cité », ses citations sont inconnues ;
      both          : top 25 % sur les deux axes ;
      other         : le reste.
    """
    cit, hn = F.col("citation_pct_in_year"), F.col("hn_pct")
    return df.withColumn(
        "impact_segment",
        F.when((cit >= 0.9) & ~F.col("on_hn"), "academic_only")
         .when(F.col("on_hn") & F.col("has_citations") & (hn >= 0.75) & (cit < 0.5),
               "viral_only")
         .when(F.col("on_hn") & (hn >= 0.75) & (cit >= 0.75), "both")
         .otherwise("other"),
    )
