"""Suites Great Expectations de la couche Silver (une par source).

Chaque expectation traduit une règle métier constatée lors de l'analyse
qualité Bronze (notebook 01). `mostly` tolère une petite fraction d'écarts
quand la règle est statistique plutôt qu'absolue.
"""

from __future__ import annotations

from datetime import UTC, datetime

import great_expectations.expectations as gxe

from scipulse.silver.keys import ARXIV_ID_PATTERN

CURRENT_YEAR = datetime.now(UTC).year
_ARXIV_ID = ARXIV_ID_PATTERN


def arxiv_suite() -> list[gxe.Expectation]:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000),
        gxe.ExpectColumnValuesToNotBeNull(column="arxiv_id"),
        gxe.ExpectColumnValuesToBeUnique(column="arxiv_id"),
        gxe.ExpectColumnValuesToMatchRegex(column="arxiv_id", regex=_ARXIV_ID),
        gxe.ExpectColumnValuesToNotBeNull(column="title"),
        gxe.ExpectColumnValueLengthsToBeBetween(column="abstract", min_value=50,
                                                max_value=10_000, mostly=0.99),
        gxe.ExpectColumnValuesToMatchRegex(column="primary_category",
                                           regex=r"^[a-z\-]+(\.[A-Za-z\-]+)?$"),
        gxe.ExpectColumnValuesToBeBetween(column="published_year", min_value=1991,
                                          max_value=CURRENT_YEAR),
        gxe.ExpectColumnValuesToBeBetween(column="n_authors", min_value=1, max_value=5_000),
        gxe.ExpectColumnValuesToMatchRegex(column="arxiv_doi", regex=r"^10\.48550/arxiv\."),
    ]


def hn_suite() -> list[gxe.Expectation]:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="hn_id"),
        gxe.ExpectColumnValuesToBeUnique(column="hn_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="title"),
        gxe.ExpectColumnValuesToBeBetween(column="score", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="num_comments", min_value=0),
        gxe.ExpectColumnValuesToBeInSet(column="source", value_set=["firebase", "algolia"]),
        gxe.ExpectColumnValuesToBeBetween(column="created_year", min_value=2006,
                                          max_value=CURRENT_YEAR),
        # Les valeurs nulles sont ignorées par GX : seule la forme des ids présents compte
        gxe.ExpectColumnValuesToMatchRegex(column="arxiv_id", regex=_ARXIV_ID),
    ]


def citations_suite() -> list[gxe.Expectation]:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="openalex_id"),
        gxe.ExpectColumnValuesToBeUnique(column="openalex_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="doi"),
        gxe.ExpectColumnValuesToMatchRegex(column="doi", regex=r"^10\.\d{4,9}/\S+$"),
        gxe.ExpectColumnValuesToBeBetween(column="cited_by_count", min_value=0),
    ]


SUITES = {
    "arxiv_silver": arxiv_suite,
    "hn_silver": hn_suite,
    "citations_silver": citations_suite,
}
