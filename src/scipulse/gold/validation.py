"""Validation de la couche Gold : contrôles d'intégrité + requêtes analytiques cibles.

Sert à deux usages :
  - dernière tâche du DAG (le pipeline n'est « vert » que si le Gold répond) ;
  - procédure de validation sur jeu de test (docs/validation.md) :
    `python -m scipulse.gold.validation` affiche les résultats des requêtes.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from scipulse.config import EsIndex, Settings
from scipulse.monitoring import Layer, MetricsRecorder

# Cas d'usage analytiques cibles, exprimés en requêtes Elasticsearch
ANALYTIC_QUERIES: dict[str, dict[str, Any]] = {
    "tres_cites_jamais_sur_hn": {
        "query": {"term": {"impact_segment": "academic_only"}},
        "sort": [{"cited_by_count": "desc"}],
    },
    "viraux_hn_peu_cites": {
        "query": {"term": {"impact_segment": "viral_only"}},
        "sort": [{"hn_points": "desc"}],
    },
    "top_impact": {
        "query": {"match_all": {}},
        "sort": [{"impact_score": "desc"}],
    },
}

GROWTH_AGG = {
    "size": 0,
    "query": {"range": {"published_year": {"gte": 2020}}},
    "aggs": {"categories": {
        "terms": {"field": "primary_category", "size": 10,
                  "order": {"growth": "desc"}, "min_doc_count": 200},
        "aggs": {"growth": {"avg": {"field": "category_yoy_growth_pct"}},
                 "impact": {"avg": {"field": "impact_score"}}},
    }},
}


def run_checks(settings: Settings | None = None, top: int = 5) -> dict[str, Any]:
    from elasticsearch import Elasticsearch

    settings = settings or Settings.from_env()
    es = Elasticsearch(settings.es_host, request_timeout=60)
    recorder = MetricsRecorder(settings.metrics_dir)
    results: dict[str, Any] = {}

    with recorder.track(pipeline="gold", layer=Layer.GOLD, source="elasticsearch",
                        task="validate_gold") as metrics:
        index = EsIndex.ARXIV_ENRICHED.value
        total = es.count(index=index)["count"]
        stats = es.search(index=index, size=0, aggs={
            "score": {"stats": {"field": "impact_score"}},
            "with_citations": {"filter": {"term": {"has_citations": True}}},
            "on_hn": {"filter": {"term": {"on_hn": True}}},
            "segments": {"terms": {"field": "impact_segment"}},
        })["aggregations"]

        checks = {
            "index_non_vide": total > 0,
            "score_dans_0_100": (stats["score"]["min"] or 0) >= 0
                                and (stats["score"]["max"] or 0) <= 100,
            "citations_presentes": stats["with_citations"]["doc_count"] > 0,
            "hn_present": stats["on_hn"]["doc_count"] > 0,
        }
        results["documents"] = total
        results["score"] = stats["score"]
        results["segments"] = {b["key"]: b["doc_count"] for b in stats["segments"]["buckets"]}

        for name, body in ANALYTIC_QUERIES.items():
            hits = es.search(index=index, size=top,
                             source=["arxiv_id", "title", "cited_by_count", "hn_points",
                                     "impact_score"], **body)["hits"]["hits"]
            results[name] = [h["_source"] for h in hits]
            checks[f"requete_{name}"] = len(hits) > 0
        growth = es.search(index=index, **GROWTH_AGG)["aggregations"]["categories"]["buckets"]
        results["sous_domaines_en_croissance"] = [
            {"category": b["key"], "papers": b["doc_count"],
             "avg_growth_pct": round(b["growth"]["value"] or 0, 1),
             "avg_impact": round(b["impact"]["value"] or 0, 1)} for b in growth]
        checks["requete_croissance"] = len(growth) > 0

        results["checks"] = checks
        metrics.records_out = total
        metrics.extra = {"checks": checks}
        failed = [name for name, ok in checks.items() if not ok]
        if failed:
            raise AssertionError(f"Contrôles Gold en échec : {failed}")
    return results


if __name__ == "__main__":
    try:
        print(json.dumps(run_checks(), indent=2, ensure_ascii=False, default=str))
    except AssertionError as exc:
        print(exc)
        sys.exit(1)
