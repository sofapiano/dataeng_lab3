"""Тесты чтения чанками: границы строк не теряются и не дублируются.

Запуск:
    python tests/test_reader.py
"""

from __future__ import annotations

import io
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from mapreduce.reader import Chunk, plan_chunks, read_chunk  # noqa: E402


class FakeS3:
    """Минимальная заглушка GetObject с Range (как у настоящего S3)."""

    def __init__(self, data: bytes):
        self.data = data

    def get_object(self, Bucket, Key, Range):  # noqa: N803 - сигнатура boto3
        span = Range.split("=", 1)[1]
        start_s, end_s = span.split("-", 1)
        start, end = int(start_s), int(end_s)
        return {"Body": io.BytesIO(self.data[start:end + 1])}


def _lines_match(data: bytes, chunk_size: int) -> None:
    """Склеенные чанки должны давать ровно исходные строки."""
    client = FakeS3(data)
    chunks = plan_chunks(len(data), chunk_size)
    parts = [read_chunk(client, "b", "k", c, len(data)) for c in chunks]
    rebuilt = b"".join(parts)

    source_lines = data.split(b"\n")
    rebuilt_lines = rebuilt.split(b"\n")
    # Последний элемент — пустая строка после финального \n (или хвост без \n).
    assert rebuilt_lines[:-1] == source_lines[:-1], "строки искажены"
    assert rebuilt_lines[-1] == source_lines[-1], "потерян хвост без \\n"


def main() -> int:
    rnd = random.Random(42)

    # 1. Ровные строки и размер чанка, не кратный длине строки.
    data = b"".join(b"row%d,%s\n" % (i, b"x" * 30) for i in range(1000))
    for size in (1, 7, 64, 111, 4096, len(data), len(data) + 10):
        _lines_match(data, size)
    print("[OK] ровные строки, разные размеры чанка")

    # 2. Строки случайной длины, включая строку длиннее чанка (дочитывание хвоста).
    rows = [b"r%d," % i + b"y" * rnd.randint(0, 500) + b"\n" for i in range(500)]
    rows.append(b"LONG," + b"z" * 5000 + b"\n")
    data = b"".join(rows)
    for size in (1, 13, 100, 997, 5000, 8192):
        _lines_match(data, size)
    print("[OK] строки случайной длины, включая длиннее чанка")

    # 3. Файл без завершающего перевода строки.
    data = b"".join(rows)[:-1]
    for size in (17, 256, 4096):
        _lines_match(data, size)
    print("[OK] файл без завершающего \\n")

    # 4. Каждая строка встречается ровно один раз (нет дублей на границах).
    data = b"".join(b"%05d\n" % i for i in range(3000))
    for size in (1, 2, 3, 1000, 2999):
        client = FakeS3(data)
        chunks = plan_chunks(len(data), size)
        rows = b"".join(read_chunk(client, "b", "k", c, len(data)) for c in chunks)
        assert rows == data, f"дубли/пропуски при chunk_size={size}"
    print("[OK] нет дублей и пропусков на границах чанков")

    # 5. План чанков покрывает объект без дыр.
    for size, cs in ((847619384, 64 * 1024 * 1024), (100, 10), (1, 10)):
        chunks = plan_chunks(size, cs)
        assert chunks[0].start == 0
        assert chunks[-1].end == size - 1
        for a, b in zip(chunks, chunks[1:]):
            assert b.start == a.end + 1
        assert sum(c.nbytes for c in chunks) == size
    print("[OK] план чанков покрывает объект без дыр и пересечений")

    print("\nВсе проверки пройдены.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
