"""Reduce-фаза: суммирование частичных частот, пришедших от мапперов.

Ключи разложены по редьюсерам в mapper.py, поэтому каждый редьюсер видит
только «свою» часть пространства ключей и не пересекается с остальными.
"""

from __future__ import annotations

from collections import Counter


def reduce_partition(payload: tuple[int, int, list[dict]]) -> tuple[int, int, dict]:
    """Один редьюсер: (k, номер, частичные словари) -> (k, номер, итог)."""
    k, reducer_id, parts = payload
    total: Counter = Counter()
    for part in parts:
        total.update(part)
    return k, reducer_id, dict(total)
