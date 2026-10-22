"""Doubles de test : session HTTP scriptée (aucun appel réseau réel)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    """Renvoie la réponse calculée par `handler(url, params)` et journalise les appels."""

    def __init__(self, handler: Callable[[str, dict[str, Any]], Any]) -> None:
        self.handler = handler
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, **_: Any) -> FakeResponse:
        params = params or {}
        self.calls.append((url, params))
        return FakeResponse(self.handler(url, params))
