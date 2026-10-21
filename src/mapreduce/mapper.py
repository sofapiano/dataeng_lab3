"""Map-фаза + Shuffle: чанк -> частоты навыков, пар и троек.

Один вызов `map_chunk` — это один маппер. Он независимо от остальных:
  1. читает свой чанк из S3 (Range-GET),
  2. парсит CSV,
  3. фильтрует вакансии по правилу варианта (`common.variant`),
  4. нормализует навыки (`common.variant.normalize_skills`),
  5. генерирует ключи k = 1, 2, 3 и раскладывает их по редьюсерам
     через стабильный хеш `zlib.crc32` (встроенный `hash()` в Python
     рандомизируется при каждом старте процесса и для shuffle не годится).

Никакого состояния между мапперами нет, поэтому упавший чанк можно
перезапустить без влияния на остальные — это и проверяет main.py.
"""

from __future__ import annotations

import csv
import io
import os
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import combinations

from common.config import Settings
from common.s3 import make_s3_client
from common.variant import match_variant, normalize_skills

from .reader import Chunk, read_chunk

#: Максимальный размер стека навыков (1 — навык, 2 — пара, 3 — тройка).
MAX_K = 3

#: Разделитель частей составного ключа при хешировании.
PARTITION_SEP = "\x1f"

#: Колонки CSV, которые нужны мапперу.
NEEDED_COLUMNS = ("name", "role", "key_skills")


@dataclass(frozen=True)
class ChunkTask:
    """Задание мапперу: какой диапазон объекта читать и как его обрабатывать."""

    chunk: Chunk
    size: int
    columns: dict[str, int]
    reducers: int
    settings: Settings
    #: Инъекция отказа для демонстрации отказоустойчивости: none|exception|exit.
    fail_mode: str = "none"

    @property
    def index(self) -> int:
        return self.chunk.index


@dataclass
class ChunkResult:
    """Результат одного маппера."""

    index: int
    elapsed: float = 0.0
    rows: int = 0
    bad_rows: int = 0
    matched: int = 0
    with_skills: int = 0
    #: Распределение «число навыков в вакансии» -> число вакансий.
    skill_hist: dict[int, int] = field(default_factory=dict)
    #: Shuffle: k -> номер редьюсера -> ключ -> частичная частота.
    counts: dict[int, dict[int, dict[tuple[str, ...], int]]] = field(default_factory=dict)


def partition_of(key: tuple[str, ...], reducers: int) -> int:
    """Стабильное разбиение ключа по редьюсерам."""
    return zlib.crc32(PARTITION_SEP.join(key).encode("utf-8")) % reducers


def iter_csv_lines(data: bytes, skip_header: bool):
    """Строки чанка как текст.

    Читаем через BytesIO, чтобы не держать в памяти вторую полную копию
    чанка в виде строки: на этой машине свободно <1 ГБ RAM.
    """
    first = True
    for raw in io.BytesIO(data):
        if first and skip_header:
            first = False
            continue
        first = False
        yield raw.decode("utf-8")


_client = None


def _s3(settings: Settings):
    global _client
    if _client is None:
        _client = make_s3_client(settings)
    return _client


def map_chunk(task: ChunkTask) -> ChunkResult:
    """Один маппер: чанк -> частичные частоты, разложенные по редьюсерам."""
    if task.fail_mode == "exit":
        # Имитация жёсткого падения процесса маппера.
        print(f"[mapper] FATAL: чанк {task.index} падает (exit 17)", flush=True)
        os._exit(17)
    if task.fail_mode == "exception":
        raise RuntimeError(f"искусственный отказ маппера на чанке {task.index}")

    started = time.perf_counter()
    settings = task.settings
    client = _s3(settings)
    data = read_chunk(client, settings.s3_bucket, settings.input_object, task.chunk, task.size)

    idx_name = task.columns["name"]
    idx_role = task.columns["role"]
    idx_skills = task.columns["key_skills"]
    # Строка с недостаточным числом колонок означает, что CSV распарсился
    # не так, как ожидалось: такие строки считаем и сообщаем наверх.
    min_cols = max(idx_name, idx_role, idx_skills) + 1

    counts = {
        k: [defaultdict(int) for _ in range(task.reducers)]
        for k in range(1, MAX_K + 1)
    }
    hist: dict[int, int] = defaultdict(int)
    result = ChunkResult(index=task.index)

    for row in csv.reader(iter_csv_lines(data, skip_header=task.index == 0)):
        if not row:
            continue
        result.rows += 1
        if len(row) < min_cols:
            result.bad_rows += 1
            continue
        if not match_variant(row[idx_name], row[idx_role]):
            continue
        result.matched += 1

        skills = normalize_skills(row[idx_skills])
        if not skills:
            continue
        result.with_skills += 1
        hist[len(skills)] += 1

        for k in range(1, min(MAX_K, len(skills)) + 1):
            for combo in combinations(skills, k):
                counts[k][partition_of(combo, task.reducers)][combo] += 1

    result.skill_hist = dict(hist)
    result.counts = {k: {r: dict(part) for r, part in enumerate(parts)} for k, parts in counts.items()}
    result.elapsed = time.perf_counter() - started
    return result
