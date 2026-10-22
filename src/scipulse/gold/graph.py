"""Option B — graphe de citations (GraphFrames) : PageRank et composantes connexes.

Sommets : articles du corpus ayant un work OpenAlex. Arêtes orientées
citant → cité, issues de `referenced_works`, restreintes aux articles du
corpus (sous-graphe induit).
Le PageRank mesure l'influence *structurelle* : être cité par des articles
eux-mêmes influents compte davantage qu'un simple nombre de citations.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GRAPHFRAMES_JAR = "graphframes-0.8.3-spark3.5-s_2.12.jar"


@dataclass
class GraphReport:
    vertices: int
    edges: int
    components: int
    largest_component: int


def citation_graph(papers: DataFrame) -> tuple[DataFrame, GraphReport]:
    """Renvoie (arxiv_id, pagerank, component_id) pour les articles du graphe."""
    import os

    spark = papers.sparkSession
    # Le module Python graphframes est embarqué dans le JAR (déjà sur le classpath)
    spark.sparkContext.addPyFile(
        os.path.join(os.environ["SPARK_HOME"], "jars", GRAPHFRAMES_JAR))
    from graphframes import GraphFrame

    vertices = (papers.where(F.col("openalex_id").isNotNull())
                .select(F.col("openalex_id").alias("id"), "arxiv_id"))
    edges = (papers.where(F.col("openalex_id").isNotNull())
             .select(F.col("openalex_id").alias("src"),
                     F.explode("referenced_works").alias("dst"))
             .join(vertices.select(F.col("id").alias("dst")), "dst", "left_semi"))

    n_edges = edges.count()
    if n_edges == 0:   # corpus trop clairsemé : pas de structure à exploiter
        empty = vertices.select("arxiv_id", F.lit(None).cast("double").alias("pagerank"),
                                F.lit(None).cast("long").alias("component_id"))
        return empty, GraphReport(vertices.count(), 0, 0, 0)

    graph = GraphFrame(vertices, edges)
    ranks = graph.pageRank(resetProbability=0.15, maxIter=10).vertices
    # Algorithme GraphX : pas besoin de répertoire de checkpoint partagé
    components = graph.connectedComponents(algorithm="graphx")
    sizes = components.groupBy("component").count()

    result = (ranks.select("id", "arxiv_id", "pagerank")
              .join(components.select("id", F.col("component").alias("component_id")), "id")
              .drop("id"))
    report = GraphReport(
        vertices=vertices.count(), edges=n_edges, components=sizes.count(),
        largest_component=sizes.agg(F.max("count")).first()[0] or 0,
    )
    return result, report
