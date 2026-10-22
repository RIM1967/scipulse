"""Normalisation des clés de jointure, partagée par les trois sources.

C'est le point de raccordement ArXiv × Hacker News × OpenAlex : une même
fonction produit la même clé partout, sinon la jointure Spark échoue
silencieusement (faux négatifs).
"""

from __future__ import annotations

import re

# Identifiants ArXiv : nouveau format (depuis 2007) « 2003.01234 » et ancien
# format « cs/0701001 » ou « math.CO/0601001 ». Suffixe de version « v2 » toléré.
_NEW_ID = r"\d{4}\.\d{4,5}"
_OLD_ID = r"[a-z][a-z\-]*(?:\.[A-Za-z]{2})?/\d{7}"
ARXIV_ID_PATTERN = rf"^(?:{_NEW_ID}|{_OLD_ID})$"

# URL HN pointant vers ArXiv : abs/, pdf/, html/, avec ou sans version / .pdf
_URL_RE = re.compile(
    rf"arxiv\.org/(?:abs|pdf|html|format)/({_NEW_ID}|{_OLD_ID})(?:v\d+)?(?:\.pdf)?",
    re.IGNORECASE,
)
_VERSION_RE = re.compile(r"v\d+$")
_DOI_PREFIX_RE = re.compile(r"^(?:https?://)?(?:dx\.)?doi\.org/|^doi:", re.IGNORECASE)
ARXIV_DOI_PREFIX = "10.48550/arxiv."


def normalize_arxiv_id(raw: str | None) -> str | None:
    """'2003.01234v2 ' → '2003.01234'. Renvoie None si le format est invalide."""
    if not raw:
        return None
    candidate = _VERSION_RE.sub("", raw.strip())
    return candidate if re.match(ARXIV_ID_PATTERN, candidate) else None


def arxiv_id_from_url(url: str | None) -> str | None:
    """Extrait l'identifiant ArXiv d'une URL (stories Hacker News)."""
    if not isinstance(url, str) or not url:   # None, ou NaN venant de pandas
        return None
    match = _URL_RE.search(url)
    return normalize_arxiv_id(match.group(1)) if match else None


def normalize_doi(raw: str | None) -> str | None:
    """DOI canonique : minuscules, sans préfixe https://doi.org/.

    Anomalies réelles du dump ArXiv (cf. notebook qualité) :
      - plusieurs DOI dans le même champ, séparés par des espaces → on garde le premier ;
      - échappements LaTeX « \\_ » → « _ ».
    """
    if not raw or not raw.strip():
        return None
    first = re.split(r"[\s,;]+", raw.strip())[0].replace("\\", "")
    doi = _DOI_PREFIX_RE.sub("", first).strip().lower()
    return doi if doi.startswith("10.") else None


def arxiv_doi(arxiv_id: str) -> str:
    """DOI DataCite attribué par ArXiv à chaque preprint depuis 2022 (rétroactivement)."""
    return f"{ARXIV_DOI_PREFIX}{arxiv_id}".lower()


def arxiv_id_from_doi(doi: str | None) -> str | None:
    """'10.48550/arxiv.2003.01234' → '2003.01234' (inverse de arxiv_doi)."""
    doi = normalize_doi(doi)
    if not doi or not doi.startswith(ARXIV_DOI_PREFIX):
        return None
    return normalize_arxiv_id(doi.removeprefix(ARXIV_DOI_PREFIX))
