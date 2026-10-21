"""Этап 4. Верификация: результаты MapReduce и DuckDB должны совпасть.

Скрипт скачивает оба набора результатов ИЗ ХРАНИЛИЩА (а не берёт локальные
файлы) и сравнивает их запросом FULL OUTER JOIN в DuckDB по каждому k.

Запуск:
    python scripts/verify.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb  # noqa: E402

from common.config import load_settings  # noqa: E402
from common.hashing import md5_file  # noqa: E402
from common.reporting import Transcript  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402

SQL_FILE = REPO_ROOT / "src" / "sql" / "verify_mapreduce_vs_duckdb.sql"

KEY_CONCAT = {
    1: ("skill", "skill1"),
    2: ("concat_ws(chr(31), skill1, skill2)",) * 2,
    3: ("concat_ws(chr(31), skill1, skill2, skill3)",) * 2,
}

OBJECTS = {
    "mr/skills_k1.csv": "skills_k1.csv",
    "mr/skills_k2.csv": "skills_k2.csv",
    "mr/skills_k3.csv": "skills_k3.csv",
    "duckdb/skills_k1.csv": "skills_k1.csv",
    "duckdb/skills_k2.csv": "skills_k2.csv",
    "duckdb/skills_k3.csv": "skills_k3.csv",
}


def download(client, settings, local_dir: Path) -> dict[str, Path]:
    """Качает результаты из бакета; возвращает 'источник/файл' -> локальный путь."""
    local_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for source_file, name in OBJECTS.items():
        source, filename = source_file.split("/")
        key = f"{settings.s3_prefix_results}{source}/{filename}"
        target = local_dir / source / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(settings.s3_bucket, key, str(target))
        paths[source_file] = target
    return paths


def content_signature(path: Path) -> tuple[str, int]:
    """MD5 и число строк файла (для лога)."""
    with path.open("rb") as fh:
        lines = sum(1 for _ in fh)
    return md5_file(path), lines


def main() -> int:
    settings = load_settings()
    client = make_s3_client(settings)
    local_dir = Path("data/intermediate/verify")
    transcript = Path(settings.report_dir) / "stage4_verify.txt"
    template = SQL_FILE.read_text(encoding="utf-8")

    with Transcript(transcript) as t:
        t.rule("ЭТАП 4. ВЕРИФИКАЦИЯ: MapReduce vs DuckDB")
        t.log()
        t.log("  Результаты скачиваются из хранилища:")
        for source_file in OBJECTS:
            source, filename = source_file.split("/")
            t.log(f"    s3://{settings.s3_bucket}/{settings.s3_prefix_results}{source}/{filename}")
        t.log()

        paths = download(client, settings, local_dir)

        t.rule("1. Контрольные суммы скачанных файлов")
        for source_file, path in paths.items():
            md5, lines = content_signature(path)
            t.log(f"  {source_file:<22} строк {lines:>7}  MD5={md5}")
        t.log()

        con = duckdb.connect(config={"memory_limit": settings.duckdb_memory_limit,
                                     "threads": settings.duckdb_threads})
        t.rule("2. Сверка FULL OUTER JOIN (0/0/0 = полное совпадение)")
        t.log(f"  {'k':>2} {'ключей MR':>10} {'ключей DB':>10} {'только MR':>10} "
              f"{'только DB':>10} {'разные cnt':>11} {'сумма MR':>10} {'сумма DB':>10}")
        all_ok = True
        for k in (1, 2, 3):
            mr_key, db_key = KEY_CONCAT[k]
            sql = (template
                   .replace("{{MR_KEY}}", mr_key)
                   .replace("{{DB_KEY}}", db_key)
                   .replace("{{MR_PATH}}", paths[f"mr/skills_k{k}.csv"].as_posix())
                   .replace("{{DB_PATH}}", paths[f"duckdb/skills_k{k}.csv"].as_posix()))
            only_mr, only_db, diff, total, mr_total, db_total = con.execute(sql).fetchone()
            ok = only_mr == 0 and only_db == 0 and diff == 0 and mr_total == db_total
            all_ok &= ok
            t.log(f"  {k:>2} {total:>10} {total:>10} {only_mr:>10} {only_db:>10} "
                  f"{diff:>11} {mr_total:>10} {db_total:>10}")
        t.log()

        t.rule("3. Сверка гистограммы «число навыков в вакансии»")
        hist_sql = """
            WITH mr AS (
                SELECT CAST(skills_per_vacancy AS INT) AS n, CAST(vacancies AS BIGINT) AS mr_v
                FROM read_csv('{mr}', header=true, all_varchar=true)
            ), db AS (
                SELECT CAST(skills_per_vacancy AS INT) AS n, CAST(vacancies AS BIGINT) AS db_v
                FROM read_csv('{db}', header=true, all_varchar=true)
            )
            SELECT count(*) FILTER (WHERE mr_v IS DISTINCT FROM db_v) AS diff, count(*) AS total
            FROM mr FULL OUTER JOIN db ON mr.n = db.n
        """
        mr_hist = local_dir / "mr" / "skill_histogram.csv"
        db_hist = local_dir / "duckdb" / "skill_histogram.csv"
        for source, target in (("mr", mr_hist), ("duckdb", db_hist)):
            target.parent.mkdir(parents=True, exist_ok=True)
            key = f"{settings.s3_prefix_results}{source}/skill_histogram.csv"
            client.download_file(settings.s3_bucket, key, str(target))
        diff, total = con.execute(
            hist_sql.format(mr=mr_hist.as_posix(), db=db_hist.as_posix())
        ).fetchone()
        t.log(f"  строк гистограммы: {total}, расхождений: {diff}")
        all_ok &= diff == 0
        t.log()

        t.rule("4. Повтор после отказа маппера (локальные файлы)")
        mr_dir = Path(settings.output_dir) / "mr"
        for k in (1, 2, 3):
            clean = mr_dir / f"skills_k{k}.csv"
            retried = mr_dir / f"skills_k{k}_failure.csv"
            if not retried.is_file():
                t.log(f"  k={k}: файла после отказа нет (запустите main.py --fail-chunk ...)")
                continue
            same = md5_file(clean) == md5_file(retried)
            all_ok &= same
            t.log(f"  k={k}: MD5 обычного запуска {'==' if same else '!='} MD5 запуска "
                  f"с падением маппера ({'совпадает' if same else 'РАСХОЖДЕНИЕ'})")
        t.log()

        t.rule("ИТОГ ЭТАПА 4")
        t.log("  [OK] MapReduce и DuckDB дают полностью совпадающие результаты"
              if all_ok else "  [FAIL] найдены расхождения — см. таблицу выше")
        t.log(f"  транскрипт: {transcript}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
