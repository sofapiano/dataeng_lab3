"""Этап 3а. MapReduce по объекту в S3: частоты навыков, пар и троек.

Запуск:
    python src/mapreduce/main.py
    python src/mapreduce/main.py --workers 3 --chunk-size-mb 32

Демонстрация отказоустойчивости (падение одного маппера + повтор чанка):
    python src/mapreduce/main.py --fail-chunk 6 --fail-mode exit --tag _failure \
        --out reports/output/stage3_mapreduce_failure.txt

Результаты (локально и в бакете, если не указан --no-upload):
    data/results/mr/skills_k1.csv, skills_k2.csv, skills_k3.csv
    s3://<bucket>/results/mr/skills_k1.csv, ...
    data/results/mr/skill_histogram.csv, summary.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import replace
from math import comb
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from common.config import Settings, load_settings  # noqa: E402
from common.reporting import Transcript  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402
from common.variant import VARIANT  # noqa: E402
from mapreduce.mapper import (  # noqa: E402
    MAX_K,
    NEEDED_COLUMNS,
    ChunkResult,
    ChunkTask,
    map_chunk,
)
from mapreduce.reader import plan_chunks, read_range  # noqa: E402
from mapreduce.reducer import reduce_partition  # noqa: E402

K_COLUMNS = {
    1: ["skill"],
    2: ["skill1", "skill2"],
    3: ["skill1", "skill2", "skill3"],
}


def human(n: float) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "Б" else f"{int(n)} Б"
        n /= 1024
    return f"{n:.1f} ПБ"


def read_header(client, settings: Settings) -> list[str]:
    """Заголовок CSV одним маленьким Range-запросом."""
    head = read_range(client, settings.s3_bucket, settings.input_object, 0, 4095)
    first_line = head.split(b"\n", 1)[0]
    return next(csv.reader([first_line.decode("utf-8")]))


def map_phase(
    tasks: list[ChunkTask],
    *,
    workers: int,
    max_attempts: int,
    fail_chunk: int | None,
    fail_mode: str,
    t: Transcript,
) -> tuple[dict[int, ChunkResult], int]:
    """Map-фаза с повтором упавших чанков.

    Отказ имитируется только на ПЕРВОЙ попытке указанного чанка: повторный
    запуск уже штатный. Так проверяется, что повтор даёт тот же результат.
    """
    remaining = {task.index: task for task in tasks}
    done: dict[int, ChunkResult] = {}
    attempts = {task.index: 0 for task in tasks}
    retries = 0

    while remaining:
        batch: dict[int, ChunkTask] = {}
        for idx, task in remaining.items():
            attempts[idx] += 1
            if attempts[idx] > max_attempts:
                raise RuntimeError(f"чанк {idx}: превышено число попыток ({max_attempts})")
            mode = fail_mode if (idx == fail_chunk and attempts[idx] == 1) else "none"
            batch[idx] = replace(task, fail_mode=mode)

        executor = ProcessPoolExecutor(max_workers=workers)
        broken = False
        try:
            futures = {executor.submit(map_chunk, task): idx for idx, task in batch.items()}
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    result = fut.result()
                except BrokenProcessPool:
                    # Процесс маппера убит (os._exit). Пул неработоспособен:
                    # пересоздаём его и повторяем все незавершённые чанки.
                    broken = True
                    retries += 1
                    cause = (f"инъекция отказа: чанк {fail_chunk}"
                             if fail_chunk is not None else "причина неизвестна")
                    t.log(f"  [ОТКАЗ] аварийно завершился процесс маппера ({cause}); "
                          f"потерян результат чанка {idx:02d} "
                          f"(попытка {attempts[idx]}/{max_attempts}); "
                          f"пул будет пересоздан")
                    break
                except Exception as exc:  # noqa: BLE001
                    retries += 1
                    t.log(f"  [ОТКАЗ] маппер чанка {idx:02d} упал: "
                          f"{type(exc).__name__}: {exc} "
                          f"(попытка {attempts[idx]}/{max_attempts})")
                    if attempts[idx] >= max_attempts:
                        raise
                    continue
                remaining.pop(idx)
                done[idx] = result
                chunk = batch[idx].chunk
                t.log(
                    f"  [OK] чанк {idx:02d} байты [{chunk.start}..{chunk.end}] "
                    f"({human(chunk.nbytes)}): строк {result.rows}, "
                    f"вакансий варианта {result.matched}, из них с навыками "
                    f"{result.with_skills}, {result.elapsed:.1f} с"
                )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        if broken:
            t.log(f"  [i] пул мапперов пересоздан, незавершённых чанков: {len(remaining)}")

    return done, retries


def reduce_phase(
    results: dict[int, ChunkResult], reducers: int, t: Transcript
) -> dict[int, dict[tuple[str, ...], int]]:
    """Shuffle + Reduce: раскладываем частичные частоты и суммируем."""
    shuffle: dict[tuple[int, int], list[dict]] = {
        (k, r): [] for k in range(1, MAX_K + 1) for r in range(reducers)
    }
    for result in results.values():
        for k, by_reducer in result.counts.items():
            for r, part in by_reducer.items():
                shuffle[(k, r)].append(part)

    reduced: dict[int, dict[tuple[str, ...], int]] = {k: {} for k in range(1, MAX_K + 1)}
    with ProcessPoolExecutor(max_workers=reducers) as executor:
        futures = {
            executor.submit(reduce_partition, (k, r, parts)): (k, r)
            for (k, r), parts in shuffle.items()
        }
        for fut in as_completed(futures):
            k, r, counts = fut.result()
            t.log(f"  редьюсер {r} (k={k}): {len(counts)} уникальных ключей")
            overlap = reduced[k].keys() & counts.keys()
            if overlap:
                raise RuntimeError(f"пересечение ключей между редьюсерами k={k}: {len(overlap)}")
            reduced[k].update(counts)
    return reduced


def write_results(
    reduced: dict[int, dict[tuple[str, ...], int]],
    hist: dict[int, int],
    out_dir: Path,
    tag: str,
) -> dict[str, Path]:
    """Пишет CSV с результатами (сортировка: частота убыв., затем ключ)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    for k in range(1, MAX_K + 1):
        path = out_dir / f"skills_k{k}{tag}.csv"
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow([*K_COLUMNS[k], "count"])
            for key, count in sorted(reduced[k].items(), key=lambda kv: (-kv[1], kv[0])):
                writer.writerow([*key, count])
        paths[f"skills_k{k}"] = path

    hist_path = out_dir / f"skill_histogram{tag}.csv"
    with hist_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["skills_per_vacancy", "vacancies"])
        for n in sorted(hist):
            writer.writerow([n, hist[n]])
    paths["skill_histogram"] = hist_path

    summary_path = out_dir / f"summary{tag}.csv"
    with summary_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["k", "unique_keys", "total_occurrences", "expected_from_histogram"])
        for k in range(1, MAX_K + 1):
            expected = sum(comb(n, k) * v for n, v in hist.items() if n >= k)
            writer.writerow([k, len(reduced[k]), sum(reduced[k].values()), expected])
    paths["summary"] = summary_path
    return paths


def md5_of(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    client = make_s3_client(settings)
    chunk_size = args.chunk_size_mb * 1024 * 1024
    out_dir = Path(settings.output_dir) / "mr"
    transcript = Path(args.out) if args.out else Path(settings.report_dir) / "stage3_mapreduce.txt"

    head = client.head_object(Bucket=settings.s3_bucket, Key=settings.input_object)
    size = head["ContentLength"]
    chunks = plan_chunks(size, chunk_size)
    header = read_header(client, settings)
    columns = {name: header.index(name) for name in NEEDED_COLUMNS}
    tasks = [
        ChunkTask(chunk=chunk, size=size, columns=columns, reducers=args.reducers, settings=settings)
        for chunk in chunks
    ]

    started_all = time.perf_counter()
    with Transcript(transcript) as t:
        t.rule("ЭТАП 3а. MapReduce: частоты навыков, пар и троек (данные из S3)")
        t.log()
        t.log(f"  вариант     : {VARIANT['number']} — {VARIANT['title']}")
        t.log(f"  правило     : role = '{VARIANT['role']}' AND {VARIANT['name_regex']}")
        t.log(f"  объект      : s3://{settings.s3_bucket}/{settings.input_object}")
        t.log(f"  размер      : {human(size)} ({size} байт)")
        t.log(f"  чанков      : {len(chunks)} по {args.chunk_size_mb} МБ (Range-GET)")
        t.log(f"  мапперов    : {args.workers}, редьюсеров: {args.reducers}")
        t.log(f"  колонки     : {columns}")
        if args.fail_chunk is not None:
            t.log(f"  ИНЪЕКЦИЯ ОТКАЗА: чанк {args.fail_chunk}, режим '{args.fail_mode}', "
                  f"попыток на чанк: {args.max_attempts}")
        t.log()

        t.rule("1. MAP: параллельное чтение чанков из S3 и подсчёт")
        results, retries = map_phase(
            tasks,
            workers=args.workers,
            max_attempts=args.max_attempts,
            fail_chunk=args.fail_chunk,
            fail_mode=args.fail_mode,
            t=t,
        )
        t.log(f"  итого чанков обработано: {len(results)}, повторных запусков: {retries}")
        t.log()

        total_rows = sum(r.rows for r in results.values())
        total_bad = sum(r.bad_rows for r in results.values())
        total_matched = sum(r.matched for r in results.values())
        total_with_skills = sum(r.with_skills for r in results.values())
        hist: Counter[int] = Counter()
        for r in results.values():
            hist.update(r.skill_hist)
        hist_dict = dict(sorted(hist.items()))

        t.rule("2. Контроль разбора CSV")
        t.log(f"  строк данных распознано : {total_rows}")
        t.log(f"  строк с нехваткой колонок: {total_bad}")
        t.log(f"  вакансий варианта       : {total_matched}")
        t.log(f"  из них с навыками       : {total_with_skills}")
        t.log(f"  без навыков             : {total_matched - total_with_skills}")
        t.log()

        t.rule("3. SHUFFLE + REDUCE: суммирование частот по редьюсерам")
        reduced = reduce_phase(results, args.reducers, t)
        t.log()

        t.rule("4. Рост числа ключей при увеличении стека (число сочетаний)")
        t.log(f"  {'k':>2} {'уникальных ключей':>18} {'вхождений':>12} {'ожидается из гистограммы':>26}")
        ok_expected = True
        for k in range(1, MAX_K + 1):
            expected = sum(comb(n, k) * v for n, v in hist_dict.items() if n >= k)
            actual = sum(reduced[k].values())
            ok_expected &= expected == actual
            t.log(f"  {k:>2} {len(reduced[k]):>18} {actual:>12} {expected:>26}")
        t.log()
        t.log("  Пояснение: вакансия с n навыками даёт C(n,k) ключей. Поэтому суммы")
        t.log("  вхождений для k=2 и k=3 больше числа вакансий, а число уникальных")
        t.log("  ключей растёт медленнее: разные вакансии переиспользуют комбинации.")
        t.log(f"  Совпадение фактических вхождений с C(n,k) по гистограмме: "
              f"{'ДА' if ok_expected else 'НЕТ'}")
        t.log()
        t.log("  Гистограмма «число навыков в вакансии»:")
        for n, v in hist_dict.items():
            t.log(f"    {n:>2} навык(ов): {v}")
        t.log()

        t.rule(f"5. ТОП-{args.top} ключей")
        for k in range(1, MAX_K + 1):
            t.log(f"  k={k}:")
            for key, count in sorted(reduced[k].items(), key=lambda kv: (-kv[1], kv[0]))[: args.top]:
                t.log(f"    {' + '.join(key):<70} {count:>6}")
            t.log()

        t.rule("6. Запись результатов")
        paths = write_results(reduced, hist_dict, out_dir, args.tag)
        for name, path in paths.items():
            t.log(f"  {name:<18} {path}  ({human(path.stat().st_size)}), MD5={md5_of(path)}")
        t.log()

        if not args.no_upload:
            t.rule("7. Загрузка результатов в объектное хранилище")
            for name, path in paths.items():
                key = f"{settings.s3_prefix_results}mr/{path.name}"
                client.put_object(
                    Bucket=settings.s3_bucket,
                    Key=key,
                    Body=path.read_bytes(),
                    ContentType="text/csv",
                )
                obj = client.head_object(Bucket=settings.s3_bucket, Key=key)
                t.log(f"  [OK] s3://{settings.s3_bucket}/{key} "
                      f"({human(obj['ContentLength'])})")
            t.log()

        t.rule("ИТОГ ЭТАПА 3а")
        t.log(f"  вакансий варианта: {total_matched}, с навыками: {total_with_skills}")
        t.log(f"  уникальных навыков / пар / троек: "
              f"{len(reduced[1])} / {len(reduced[2])} / {len(reduced[3])}")
        t.log(f"  время: {time.perf_counter() - started_all:.1f} с")
        t.log(f"  транскрипт: {transcript}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Этап 3а: MapReduce поверх S3")
    parser.add_argument("--chunk-size-mb", type=int, default=settings.chunk_size_mb)
    parser.add_argument("--workers", type=int, default=settings.workers)
    parser.add_argument("--reducers", type=int, default=settings.reducers)
    parser.add_argument("--fail-chunk", type=int, default=None,
                        help="номер чанка, на первой попытке которого имитируется отказ")
    parser.add_argument("--fail-mode", choices=["exception", "exit"], default="exit")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--tag", default="", help="суффикс имён файлов результатов")
    parser.add_argument("--no-upload", action="store_true", help="не заливать результаты в S3")
    parser.add_argument("--out", default=None, help="файл транскрипта")
    parser.add_argument("--top", type=int, default=10)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
