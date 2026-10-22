"""Session HTTP commune aux API externes (Hacker News, Algolia, OpenAlex).

Retries avec backoff exponentiel sur erreurs réseau, 429 (quota) et 5xx :
une API publique instable ne doit pas faire échouer tout un DAG.
"""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "SciPulse/0.1 (projet pédagogique de veille scientifique)"


def make_session(retries: int = 5, backoff: float = 1.0, pool_size: int = 32) -> requests.Session:
    """Session réutilisable (keep-alive) avec politique de retry."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff,          # 1s, 2s, 4s, 8s...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = USER_AGENT
    return session
