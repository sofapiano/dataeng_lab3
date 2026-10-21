"""Подсчёт контрольных сумм файлов и потоков."""

from __future__ import annotations

import hashlib
from typing import BinaryIO

BLOCK_SIZE = 1 << 22  # 4 МиБ


def md5_file(path, block: int = BLOCK_SIZE) -> str:
    """MD5 файла на диске."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(block):
            h.update(chunk)
    return h.hexdigest()


def md5_stream(stream: BinaryIO, block: int = BLOCK_SIZE) -> tuple[str, int, int]:
    """MD5, размер и число переводов строк для потока.

    Возвращает (md5, bytes, newlines) — третий счётчик нужен, чтобы убедиться,
    что CSV не обрезан на границе строки.
    """
    h = hashlib.md5()
    total = 0
    newlines = 0
    while chunk := stream.read(block):
        h.update(chunk)
        total += len(chunk)
        newlines += chunk.count(b"\n")
    return h.hexdigest(), total, newlines
