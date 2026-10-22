"""Fouille de texte distribuée (Spark MLlib) sur titre + abstract.

1. TF-IDF (obligatoire) : RegexTokenizer → StopWordsRemover → CountVectorizer
   → IDF. CountVectorizer plutôt que HashingTF : il conserve le vocabulaire,
   indispensable pour restituer des mots-clés lisibles dans Kibana.
2. Option C — clustering thématique : KMeans (distance cosinus, adaptée aux
   vecteurs TF-IDF de longueurs variables) dont le k est choisi par le score
   de silhouette ; PCA à 2 composantes pour la carte x_pca / y_pca.
   Le clustering utilise un vocabulaire réduit (2 000 termes) : la PCA MLlib
   calcule une matrice de covariance d×d sur le driver (2 000² ≈ 32 Mo,
   contre 3,2 Go pour 20 000 termes).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import (
    IDF,
    PCA,
    CountVectorizer,
    Normalizer,
    RegexTokenizer,
    StopWordsRemover,
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# Mots fréquents dans tout abstract scientifique, sans valeur discriminante
DOMAIN_STOPWORDS = [
    "paper", "propose", "proposed", "proposes", "approach", "method", "methods",
    "results", "result", "show", "shows", "using", "based", "also", "use", "used",
    "new", "novel", "work", "present", "problem", "problems", "existing", "however",
    "well", "first", "two", "can", "may", "via", "large", "performance", "different",
]


def text_features(df: DataFrame, vocab_size: int, prefix: str) -> Pipeline:
    return Pipeline(stages=[
        RegexTokenizer(inputCol="text", outputCol=f"{prefix}_tokens", pattern="[^a-z]+",
                       minTokenLength=3, toLowercase=True),
        StopWordsRemover(inputCol=f"{prefix}_tokens", outputCol=f"{prefix}_terms",
                         stopWords=StopWordsRemover.loadDefaultStopWords("english")
                         + DOMAIN_STOPWORDS),
        CountVectorizer(inputCol=f"{prefix}_terms", outputCol=f"{prefix}_tf",
                        vocabSize=vocab_size, minDF=5, maxDF=0.5),
        IDF(inputCol=f"{prefix}_tf", outputCol=f"{prefix}_tfidf"),
    ])


def with_text(df: DataFrame) -> DataFrame:
    return df.withColumn("text", F.concat_ws(" ", "title", "abstract"))


def tfidf_keywords(df: DataFrame, vocab_size: int = 20_000, top_k: int = 8
                   ) -> tuple[DataFrame, PipelineModel]:
    """Ajoute `keywords` : les termes de plus fort poids TF-IDF de chaque article."""
    model = text_features(df, vocab_size, "kw").fit(df)
    vocabulary = model.stages[2].vocabulary
    vocab = df.sparkSession.sparkContext.broadcast(vocabulary)

    @F.udf(ArrayType(StringType()))
    def top_terms(vector):  # vector : SparseVector MLlib
        if vector is None or vector.numNonzeros() == 0:
            return []
        ranked = sorted(zip(vector.indices, vector.values, strict=True),
                        key=lambda iv: -iv[1])[:top_k]
        return [vocab.value[int(i)] for i, _ in ranked]

    out = (model.transform(df)
           .withColumn("keywords", top_terms("kw_tfidf"))
           .drop("kw_tokens", "kw_terms", "kw_tf", "kw_tfidf"))
    return out, model


@dataclass
class ClusteringReport:
    k: int
    silhouette_by_k: dict[int, float]
    pca_explained_variance: list[float]
    cluster_labels: dict[int, str]
    cluster_dominant_category: dict[int, dict[str, float | str]] = field(default_factory=dict)


def topic_clusters(df: DataFrame, k_candidates: tuple[int, ...] = (8, 12, 16, 20),
                   vocab_size: int = 2_000, seed: int = 42
                   ) -> tuple[DataFrame, ClusteringReport]:
    """Option C : KMeans (k par silhouette) + PCA 2D + libellé de cluster."""
    features = text_features(df, vocab_size, "cl").fit(df)
    vocabulary = features.stages[2].vocabulary
    vectors = Normalizer(inputCol="cl_tfidf", outputCol="features").transform(
        features.transform(df)).cache()

    evaluator = ClusteringEvaluator(distanceMeasure="cosine")
    sample = vectors.sample(fraction=min(1.0, 20_000 / max(vectors.count(), 1)), seed=seed)
    scores: dict[int, float] = {}
    models = {}
    for k in k_candidates:
        models[k] = KMeans(k=k, seed=seed, distanceMeasure="cosine", maxIter=30).fit(vectors)
        scores[k] = round(evaluator.evaluate(models[k].transform(sample)), 4)
    best_k = max(scores, key=scores.get)
    kmeans = models[best_k]

    labels = {
        i: ", ".join(vocabulary[j] for j in center.argsort()[::-1][:4])
        for i, center in enumerate(kmeans.clusterCenters())
    }
    pca = PCA(k=2, inputCol="features", outputCol="pca").fit(vectors)

    label_map = F.create_map(*[x for i, label in labels.items()
                               for x in (F.lit(i), F.lit(label))])
    out = (pca.transform(kmeans.transform(vectors))
           .withColumnRenamed("prediction", "cluster_id")
           .withColumn("cluster_label", label_map[F.col("cluster_id")])
           .withColumn("_pca", vector_to_array("pca"))
           .withColumn("x_pca", F.round(F.col("_pca")[0], 5))
           .withColumn("y_pca", F.round(F.col("_pca")[1], 5))
           .drop("cl_tokens", "cl_terms", "cl_tf", "cl_tfidf", "features", "pca", "_pca"))

    # Comparaison clusters ↔ catégories ArXiv déclarées (pureté par cluster)
    counts = out.groupBy("cluster_id", "primary_category").count()
    totals = counts.groupBy("cluster_id").agg(F.sum("count").alias("total"))
    dominant = {
        r["cluster_id"]: {"category": r["primary_category"],
                          "share": round(r["count"] / r["total"], 3)}
        for r in (counts.join(totals, "cluster_id")
                  .withColumn("_r", F.row_number().over(
                      Window.partitionBy("cluster_id").orderBy(F.desc("count"))))
                  .where("_r = 1").collect())
    }
    vectors.unpersist()
    return out, ClusteringReport(best_k, scores,
                                 [round(float(v), 4) for v in pca.explainedVariance],
                                 labels, dominant)
