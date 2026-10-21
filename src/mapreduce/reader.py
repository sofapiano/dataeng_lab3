"""Чтение объекта из S3 чанками (байтовыми диапазонами).

Данные читаются именно из хранилища: каждый чанк — отдельный HTTP Range-запрос
(`GetObject` с заголовком `Range`), поэтому чанки можно тянуть параллельно из
разных процессов.

Правило владения строкой: чанк `[start, end]` обрабатывает строки, которые
НАЧИНАЮТСЯ внутри этого диапазона.

  * если `start > 0`, всё до первого `\\n` — хвост строки предыдущего чанка,
    он отбрасывается;
  * если диапазон оборвался на середине строки, её остаток дочитывается
    отдельным маленьким Range-запросом (иначе строка потерялась бы: её начало
    осталось бы в этом чанке, а следующий чанк начал бы чтение с середины).

Переносов строк внутри полей CSV в датасете нет (проверено на этапе 0),
поэтому границы строк можно искать по байту `\\n`.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Шаг дочитывания хвоста строки, вышедшей за границу чанка.
TAIL_STEP = 64 * 1024


@dataclass(frozen=True)
class Chunk:
    """Байтовый диапазон объекта `[start, end]` включительно."""

    index: int
    start: int
    end: int

    @property
    def nbytes(self) -> int:
        return self.end - self.start + 1


def plan_chunks(size: int, chunk_size: int) -> list[Chunk]:
    """Делит объект на диапазоны по `chunk_size` байт."""
    if size <= 0 or chunk_size <= 0:
        return []
    return [
        Chunk(index=i, start=start, end=min(start + chunk_size, size) - 1)
        for i, start in enumerate(range(0, size, chunk_size))
    ]


def read_range(client, bucket: str, key: str, start: int, end: int) -> bytes:
    """Один Range-GET: байты `[start, end]` включительно."""
    resp = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    with resp["Body"] as body:
        return body.read()


def read_chunk(client, bucket: str, key: str, chunk: Chunk, size: int) -> bytes:
    """Байты чанка, выровненные по границам строк (см. docstring модуля)."""
    if chunk.start == 0:
        data = read_range(client, bucket, key, 0, chunk.end)
    else:
        # Читаем на один байт раньше: по нему определяем, начинается ли
        # строка ровно на границе чанка или байт `start` — её середина.
        data = read_range(client, bucket, key, chunk.start - 1, chunk.end)
        if data[0:1] != b"\n":
            # Байт `start` — середина строки предыдущего чанка: отбрасываем её.
            nl = data.find(b"\n")
            if nl < 0:
                # Весь диапазон — продолжение одной длинной строки: она целиком
                # принадлежит предыдущему чанку.
                return b""
            data = data[nl + 1:]
        else:
            data = data[1:]

    # Дочитываем остаток последней строки, если она вышла за границу чанка.
    if not data or data.endswith(b"\n") or chunk.end + 1 >= size:
        return data

    pos = chunk.end + 1
    while pos < size:
        extra = read_range(client, bucket, key, pos, min(pos + TAIL_STEP, size) - 1)
        if not extra:
            break
        nl = extra.find(b"\n")
        if nl >= 0:
            data += extra[: nl + 1]
            break
        data += extra
        pos += len(extra)
    return data
