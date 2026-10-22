"""Dashboards Kibana « as code » : génération, import et export des saved objects.

  python scripts/kibana_objects.py build    → kibana/scipulse-dashboards.ndjson
  python scripts/kibana_objects.py import   → importe le .ndjson dans Kibana
  python scripts/kibana_objects.py export   → ré-exporte depuis Kibana (après
                                              retouches manuelles dans l'UI)

Pourquoi générer plutôt que cliquer ? Les dashboards sont versionnés, relus
en revue de code et reproductibles : `make up` sur une machine vierge les
recrée à l'identique (service `kibana-init`).

Visualisations : agrégations Kibana classiques (métriques, tableaux, nuage
de mots) et Vega-Lite pour les graphiques (barres, courbes, nuages de
points). `%context%: true` branche les graphiques Vega sur la barre de
recherche, les filtres et la plage de temps du dashboard.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
NDJSON = ROOT / "kibana" / "scipulse-dashboards.ndjson"
KIBANA_URL = os.environ.get("KIBANA_URL", "http://localhost:5601")

ARXIV, GOLD, METRICS = "arxiv-papers", "arxiv-papers-enriched", "scipulse-pipeline-metrics"
DATA_VIEWS = {
    ARXIV: ("dv-arxiv-papers", "ArXiv — Gold partiel (Module 1)", "published_at"),
    GOLD: ("dv-arxiv-enriched", "ArXiv enrichi — Gold (Module 2)", "published_at"),
    METRICS: ("dv-pipeline-metrics", "Monitoring du pipeline", "@timestamp"),
}
VEGA_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json"


# ── Briques de saved objects ─────────────────────────────────────────────────

def data_view(index: str) -> dict[str, Any]:
    object_id, name, time_field = DATA_VIEWS[index]
    return {"type": "index-pattern", "id": object_id, "references": [],
            "attributes": {"title": index, "name": name, "timeFieldName": time_field}}


def search_source(index: str | None, query: str = "") -> tuple[str, list[dict[str, str]]]:
    source: dict[str, Any] = {"query": {"query": query, "language": "kuery"}, "filter": []}
    refs = []
    if index:
        source["indexRefName"] = "kibanaSavedObjectMeta.searchSourceJSON.index"
        refs = [{"name": source["indexRefName"], "type": "index-pattern",
                 "id": DATA_VIEWS[index][0]}]
    return json.dumps(source), refs


def visualization(object_id: str, title: str, vis_type: str, *, index: str | None,
                  aggs: list[dict[str, Any]] | None = None, params: dict[str, Any] | None = None,
                  query: str = "", description: str = "") -> dict[str, Any]:
    source, refs = search_source(index, query)
    vis_state = {"title": title, "type": vis_type, "aggs": aggs or [], "params": params or {}}
    return {"type": "visualization", "id": object_id, "references": refs, "attributes": {
        "title": title, "description": description, "version": 1, "uiStateJSON": "{}",
        "visState": json.dumps(vis_state),
        "kibanaSavedObjectMeta": {"searchSourceJSON": source},
    }}


def saved_search(object_id: str, title: str, index: str, query: str,
                 columns: list[str], sort: list[list[str]]) -> dict[str, Any]:
    source, refs = search_source(index, query)
    return {"type": "search", "id": object_id, "references": refs, "attributes": {
        "title": title, "description": "", "columns": columns, "sort": sort,
        "kibanaSavedObjectMeta": {"searchSourceJSON": source},
    }}


# ── Visualisations agrégées (format Kibana classique) ────────────────────────

def agg(agg_id: str, agg_type: str, schema: str, **params: Any) -> dict[str, Any]:
    return {"id": agg_id, "enabled": True, "type": agg_type, "schema": schema, "params": params}


def metric(object_id: str, title: str, index: str, metric_agg: dict[str, Any],
           query: str = "") -> dict[str, Any]:
    params = {"addTooltip": True, "addLegend": False, "type": "metric", "metric": {
        "percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
        "metricColorMode": "None", "colorsRange": [{"from": 0, "to": 10000}],
        "labels": {"show": True}, "invertColors": False,
        "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "",
                  "fontSize": 48}}}
    return visualization(object_id, title, "metric", index=index, aggs=[metric_agg],
                         params=params, query=query)


def table(object_id: str, title: str, index: str, aggs: list[dict[str, Any]],
          query: str = "", per_page: int = 10) -> dict[str, Any]:
    params = {"perPage": per_page, "showPartialRows": False, "showMetricsAtAllLevels": False,
              "showTotal": False, "totalFunc": "sum", "percentageCol": ""}
    return visualization(object_id, title, "table", index=index, aggs=aggs, params=params,
                         query=query)


def tagcloud(object_id: str, title: str, index: str, field: str, size: int = 40
             ) -> dict[str, Any]:
    return visualization(
        object_id, title, "tagcloud", index=index,
        aggs=[agg("1", "count", "metric"),
              agg("2", "terms", "segment", field=field, size=size, order="desc", orderBy="1")],
        params={"scale": "linear", "orientation": "single", "minFontSize": 14,
                "maxFontSize": 64, "showLabel": False})


# ── Visualisations Vega-Lite ─────────────────────────────────────────────────

def vega(object_id: str, title: str, spec: dict[str, Any], description: str = ""
         ) -> dict[str, Any]:
    # Pas de titre dans la spec : le panneau du dashboard l'affiche déjà
    spec = {"$schema": VEGA_SCHEMA, "autosize": {"type": "fit"},
            "width": "container", "height": "container", **spec}
    return visualization(object_id, title, "vega", index=None,
                         params={"spec": json.dumps(spec, indent=2)}, description=description)


def es_data(index: str, aggs: dict[str, Any], path: str, context: bool = True,
            time_field: str | None = None, query: dict[str, Any] | None = None
            ) -> dict[str, Any]:
    """Source de données Vega : agrégation ES. `context` = filtres du dashboard."""
    url: dict[str, Any] = {"index": index, "body": {"size": 0, "aggs": aggs}}
    if context:
        url["%context%"] = True
        url["%timefield%"] = time_field or DATA_VIEWS[index][2]
    elif query:
        url["body"]["query"] = query
    return {"url": url, "format": {"property": path}}


def es_hits(index: str, fields: list[str], size: int, query: dict[str, Any] | None = None
            ) -> dict[str, Any]:
    body: dict[str, Any] = {"size": size, "_source": fields}
    if query:
        body["query"] = query
    return {"url": {"index": index, "body": body}, "format": {"property": "hits.hits"}}


def bar_terms(object_id: str, title: str, index: str, field: str, label: str,
              size: int = 15) -> dict[str, Any]:
    return vega(object_id, title, {
        "data": es_data(index, {"t": {"terms": {"field": field, "size": size}}},
                        "aggregations.t.buckets"),
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "y": {"field": "key", "type": "nominal", "sort": "-x", "title": label},
            "x": {"field": "doc_count", "type": "quantitative", "title": "Articles"},
        },
    })


def lines_by_year(object_id: str, title: str, index: str, top: int = 6) -> dict[str, Any]:
    aggs = {"cat": {"terms": {"field": "primary_category", "size": top},
                    "aggs": {"year": {"histogram": {"field": "published_year", "interval": 1,
                                                    "min_doc_count": 0}}}}}
    return vega(object_id, title, {
        "data": es_data(index, aggs, "aggregations.cat.buckets"),
        "transform": [
            {"flatten": ["year.buckets"], "as": ["y"]},
            {"calculate": "datum.y.key", "as": "annee"},
            {"calculate": "datum.y.doc_count", "as": "articles"},
        ],
        "mark": {"type": "line", "point": True, "tooltip": True},
        "encoding": {
            "x": {"field": "annee", "type": "ordinal", "title": "Année de soumission"},
            "y": {"field": "articles", "type": "quantitative", "title": "Articles"},
            "color": {"field": "key", "type": "nominal", "title": "Catégorie"},
        },
    })


# ── Dashboards ───────────────────────────────────────────────────────────────

def dashboard(object_id: str, title: str, description: str,
              panels: list[tuple[dict[str, Any], int, int, int, int]],
              time_from: str, time_to: str = "now") -> dict[str, Any]:
    """panels : (saved object, x, y, largeur, hauteur) sur une grille de 48 colonnes."""
    panels_json, refs = [], []
    for i, (obj, x, y, w, h) in enumerate(panels):
        ref = f"panel_{i}"
        panels_json.append({"version": "8.13.4", "type": obj["type"], "panelIndex": str(i),
                            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
                            "embeddableConfig": {}, "panelRefName": ref})
        refs.append({"name": ref, "type": obj["type"], "id": obj["id"]})
    source, _ = search_source(None)
    return {"type": "dashboard", "id": object_id, "references": refs, "attributes": {
        "title": title, "description": description, "version": 1,
        "panelsJSON": json.dumps(panels_json),
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": True,
                                   "hidePanelTitles": False}),
        "timeRestore": True, "timeFrom": time_from, "timeTo": time_to,
        "refreshInterval": {"pause": True, "value": 0},
        "kibanaSavedObjectMeta": {"searchSourceJSON": source},
    }}


def monitoring() -> list[dict[str, Any]]:
    """Dashboard 1 — monitoring : volumes par couche, taux GX, fraîcheur, alertes."""
    idx = METRICS
    failures = 'status : "failed" or status : "quality_failed"'
    v = {
        "runs": metric("mon-runs", "Exécutions de tâches", idx, agg("1", "count", "metric")),
        "fail": metric("mon-failures", "Exécutions en échec", idx, agg("1", "count", "metric"),
                       query=failures),
        "gx": metric("mon-gx", "Taux de succès GX moyen (%)", idx,
                     agg("1", "avg", "metric", field="gx_success_percent")),
        "volume": vega("mon-volume", "Volume écrit par couche (enregistrements)", {
            "data": es_data(idx, {"layer": {"terms": {"field": "layer.keyword"}, "aggs": {
                "t": {"date_histogram": {"field": "@timestamp", "fixed_interval": "1h"},
                      "aggs": {"n": {"sum": {"field": "records_out"}}}}}}},
                "aggregations.layer.buckets"),
            "transform": [{"flatten": ["t.buckets"], "as": ["b"]},
                          {"calculate": "datum.b.key", "as": "heure"},
                          {"calculate": "datum.b.n.value", "as": "enregistrements"}],
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "x": {"field": "heure", "type": "temporal", "title": None},
                "y": {"field": "enregistrements", "type": "quantitative", "title": None},
                "color": {"field": "key", "type": "nominal", "title": "Couche",
                          "scale": {"domain": ["bronze", "silver", "gold"],
                                    "range": ["#b08d57", "#9aa5b1", "#d4af37"]}},
            },
        }),
        "freshness": table("mon-freshness", "Fraîcheur et volumes par tâche", idx, [
            agg("1", "max", "metric", field="@timestamp", customLabel="Dernière exécution"),
            agg("2", "max", "metric", field="records_out", customLabel="Dernier volume max"),
            agg("3", "avg", "metric", field="gx_success_percent", customLabel="GX moyen %"),
            agg("4", "max", "metric", field="data_watermark",
                customLabel="Donnée la plus récente"),
            agg("5", "terms", "bucket", field="task.keyword", size=20, order="desc",
                orderBy="1", customLabel="Tâche"),
        ], per_page=12),
        "alerts": table("mon-alerts", "Alertes (échecs définitifs et suites GX en échec)", idx, [
            agg("1", "count", "metric", customLabel="Occurrences"),
            agg("2", "max", "metric", field="@timestamp", customLabel="Dernière"),
            agg("3", "terms", "bucket", field="task.keyword", size=20, order="desc",
                orderBy="1", customLabel="Tâche"),
            agg("4", "terms", "bucket", field="status.keyword", size=3, order="desc",
                orderBy="1", customLabel="Statut"),
        ], query=failures),
    }
    board = dashboard("dash-monitoring", "SciPulse — Monitoring du pipeline",
                      "Volumes par couche Medallion, qualité Great Expectations, "
                      "fraîcheur des données et alertes.",
                      [(v["runs"], 0, 0, 16, 8), (v["fail"], 16, 0, 16, 8),
                       (v["gx"], 32, 0, 16, 8), (v["volume"], 0, 8, 48, 14),
                       (v["freshness"], 0, 22, 30, 16), (v["alerts"], 30, 22, 18, 16)],
                      time_from="now-7d")
    return [data_view(idx), *v.values(), board]


def exploration() -> list[dict[str, Any]]:
    """Dashboard 2 — exploration ArXiv (index arxiv-papers, Module 1)."""
    idx = ARXIV
    v = {
        "count": metric("exp-count", "Articles indexés", idx, agg("1", "count", "metric")),
        "authors": metric("exp-authors", "Auteurs par article (moyenne)", idx,
                          agg("1", "avg", "metric", field="n_authors")),
        "cats": bar_terms("exp-categories", "Distribution des catégories principales", idx,
                          "category_name", "Catégorie"),
        "years": lines_by_year("exp-years", "Évolution annuelle des publications "
                               "(6 catégories principales)", idx),
        "recent": table("exp-recent", "Article le plus récent par domaine", idx, [
            agg("1", "max", "metric", field="published_at", customLabel="Plus récent"),
            agg("2", "count", "metric", customLabel="Articles"),
            agg("3", "terms", "bucket", field="primary_category", size=40, order="desc",
                orderBy="2", customLabel="Catégorie"),
        ], per_page=10),
        "list": saved_search("exp-latest", "Articles les plus récents", idx, "",
                             ["title", "primary_category", "authors"],
                             [["published_at", "desc"]]),
    }
    board = dashboard("dash-exploration", "SciPulse — Exploration ArXiv",
                      "Distribution des catégories, évolution temporelle, articles récents. "
                      "Recherche plein texte (analyseur anglais) dans la barre KQL.",
                      [(v["count"], 0, 0, 12, 8), (v["authors"], 12, 0, 12, 8),
                       (v["cats"], 24, 0, 24, 20), (v["years"], 0, 8, 24, 20),
                       (v["recent"], 0, 28, 20, 16), (v["list"], 20, 28, 28, 16)],
                      time_from="now-12y")
    return [data_view(idx), *v.values(), board]


def insights() -> list[dict[str, Any]]:
    """Dashboard 3 — Gold augmenté (Module 2) : score d'impact et cas d'usage analytiques."""
    idx = GOLD
    v = {
        "papers": metric("ins-papers", "Articles enrichis", idx, agg("1", "count", "metric")),
        "cited": metric("ins-cited", "Avec citations OpenAlex", idx,
                        agg("1", "count", "metric"), query="has_citations : true"),
        "hn": metric("ins-hn", "Mentionnés sur Hacker News", idx, agg("1", "count", "metric"),
                     query="on_hn : true"),
        "score": metric("ins-score", "Score d'impact moyen", idx,
                        agg("1", "avg", "metric", field="impact_score")),
        "top": table("ins-top", "Top articles par score d'impact", idx, [
            agg("1", "max", "metric", field="impact_score", customLabel="Score"),
            agg("2", "max", "metric", field="cited_by_count", customLabel="Citations"),
            agg("3", "max", "metric", field="hn_points", customLabel="Points HN"),
            agg("4", "terms", "bucket", field="title.keyword", size=25, order="desc",
                orderBy="1", customLabel="Article"),
        ]),
        "academic": saved_search(
            "ins-academic-only", "Très cités académiquement, jamais sur Hacker News", idx,
            'impact_segment : "academic_only"',
            ["title", "cited_by_count", "citation_velocity", "primary_category"],
            [["cited_by_count", "desc"]]),
        "viral": saved_search(
            "ins-viral-only", "Viraux sur Hacker News, peu cités", idx,
            'impact_segment : "viral_only"',
            ["title", "hn_points", "hn_comments", "cited_by_count", "hn_top_title"],
            [["hn_points", "desc"]]),
        "scatter": vega("ins-scatter", "Citations vs signal Hacker News (articles sur HN)", {
            "data": es_hits(idx, ["title", "cited_by_count", "hn_points", "impact_segment",
                                  "impact_score"], 3000, {"term": {"on_hn": True}}),
            "transform": [
                {"calculate": "log(1 + (datum._source.cited_by_count || 0)) / LN10",
                 "as": "cit"},
                {"calculate": "log(1 + datum._source.hn_points) / LN10", "as": "hn"},
                {"calculate": "datum._source.impact_segment", "as": "segment"},
                {"calculate": "datum._source.title", "as": "titre"},
            ],
            "mark": {"type": "point", "filled": True, "opacity": 0.6},
            "encoding": {
                "x": {"field": "hn", "type": "quantitative", "title": "log10(1 + points HN)"},
                "y": {"field": "cit", "type": "quantitative", "title": "log10(1 + citations)"},
                "color": {"field": "segment", "type": "nominal", "title": "Segment"},
                "tooltip": [{"field": "titre"}, {"field": "_source.cited_by_count",
                                                  "title": "citations"},
                            {"field": "_source.hn_points", "title": "points HN"}],
            },
        }),
        "growth": vega("ins-growth", "Sous-domaines en croissance (depuis 2020) : "
                       "croissance annuelle vs impact moyen", {
            "data": es_data(idx, {"cat": {
                "terms": {"field": "primary_category", "size": 25, "min_doc_count": 100},
                "aggs": {"g": {"avg": {"field": "category_yoy_growth_pct"}},
                         "s": {"avg": {"field": "impact_score"}},
                         "h": {"sum": {"field": "hn_stories"}}}}},
                "aggregations.cat.buckets", context=False,
                query={"range": {"published_year": {"gte": 2020}}}),
            "transform": [{"calculate": "datum.g.value", "as": "croissance"},
                          {"calculate": "datum.s.value", "as": "impact"},
                          {"calculate": "datum.h.value", "as": "stories_hn"}],
            "layer": [
                {"mark": {"type": "circle", "opacity": 0.7, "tooltip": True},
                 "encoding": {"size": {"field": "doc_count", "type": "quantitative",
                                       "title": "Articles"},
                              "color": {"field": "stories_hn", "type": "quantitative",
                                        "title": "Stories HN"}}},
                {"mark": {"type": "text", "dy": -12}, "encoding": {"text": {"field": "key"}}},
            ],
            "encoding": {
                "x": {"field": "croissance", "type": "quantitative",
                      "title": "Croissance annuelle moyenne (%)"},
                "y": {"field": "impact", "type": "quantitative", "title": "Score d'impact moyen"},
            },
        }),
        "pca": vega("ins-pca", "Carte thématique (KMeans + PCA sur TF-IDF)", {
            "data": es_hits(idx, ["x_pca", "y_pca", "cluster_label", "title"], 5000,
                            {"exists": {"field": "x_pca"}}),
            "transform": [{"calculate": "datum._source.x_pca", "as": "x"},
                          {"calculate": "datum._source.y_pca", "as": "y"},
                          {"calculate": "datum._source.cluster_label", "as": "cluster"},
                          {"calculate": "datum._source.title", "as": "titre"}],
            "mark": {"type": "circle", "size": 12, "opacity": 0.5},
            "encoding": {
                "x": {"field": "x", "type": "quantitative", "title": "PC1"},
                "y": {"field": "y", "type": "quantitative", "title": "PC2"},
                "color": {"field": "cluster", "type": "nominal", "title": "Cluster"},
                "tooltip": [{"field": "titre"}, {"field": "cluster"}],
            },
        }),
        "segments": bar_terms("ins-segments", "Répartition des segments d'impact", idx,
                              "impact_segment", "Segment", size=4),
        "keywords": tagcloud("ins-keywords", "Mots-clés TF-IDF", idx, "keywords"),
        "pagerank": table("ins-pagerank", "Articles les plus influents (PageRank)", idx, [
            agg("1", "max", "metric", field="pagerank", customLabel="PageRank"),
            agg("2", "max", "metric", field="cited_by_count", customLabel="Citations"),
            agg("3", "terms", "bucket", field="title.keyword", size=15, order="desc",
                orderBy="1", customLabel="Article"),
        ]),
    }
    board = dashboard("dash-insights", "SciPulse Insights — Score d'impact (Gold)",
                      "Jointure ArXiv × OpenAlex × Hacker News : score d'impact composite, "
                      "cas d'usage analytiques, carte thématique et influence (PageRank).",
                      [(v["papers"], 0, 0, 12, 7), (v["cited"], 12, 0, 12, 7),
                       (v["hn"], 24, 0, 12, 7), (v["score"], 36, 0, 12, 7),
                       (v["top"], 0, 7, 24, 18), (v["scatter"], 24, 7, 24, 18),
                       (v["academic"], 0, 25, 24, 16), (v["viral"], 24, 25, 24, 16),
                       (v["growth"], 0, 41, 30, 20), (v["segments"], 30, 41, 18, 10),
                       (v["keywords"], 30, 51, 18, 10), (v["pca"], 0, 61, 30, 22),
                       (v["pagerank"], 30, 61, 18, 22)],
                      time_from="now-12y")
    return [data_view(idx), *v.values(), board]


def build_objects() -> list[dict[str, Any]]:
    return [*monitoring(), *exploration(), *insights()]


# ── Commandes ────────────────────────────────────────────────────────────────

def build() -> Path:
    NDJSON.parent.mkdir(parents=True, exist_ok=True)
    NDJSON.write_text("".join(json.dumps(o, ensure_ascii=False) + "\n" for o in build_objects()),
                      encoding="utf-8", newline="\n")
    return NDJSON


def import_objects(path: Path = NDJSON) -> dict[str, Any]:
    import requests

    with path.open("rb") as handle:
        response = requests.post(f"{KIBANA_URL}/api/saved_objects/_import",
                                 params={"overwrite": "true"}, headers={"kbn-xsrf": "true"},
                                 files={"file": (path.name, handle, "application/ndjson")},
                                 timeout=60)
    response.raise_for_status()
    return response.json()


def export_objects(path: Path = NDJSON) -> Path:
    import requests

    response = requests.post(f"{KIBANA_URL}/api/saved_objects/_export",
                             headers={"kbn-xsrf": "true"}, timeout=60,
                             json={"objects": [{"type": "dashboard", "id": d} for d in
                                               ("dash-monitoring", "dash-exploration",
                                                "dash-insights")],
                                   "includeReferencesDeep": True})
    response.raise_for_status()
    path.write_bytes(response.content)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["build", "import", "export"])
    args = parser.parse_args()
    if args.command == "build":
        print(f"{len(build_objects())} objets écrits dans {build()}")
    elif args.command == "import":
        result = import_objects()
        print(f"Import : {result.get('successCount')} objets, succès={result.get('success')}")
        for error in result.get("errors", []):
            print("  erreur :", error.get("id"), error.get("error"))
        return 0 if result.get("success") else 1
    else:
        print(f"Exporté vers {export_objects()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
