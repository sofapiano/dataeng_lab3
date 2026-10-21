"""Этап 3б. Подсчёт навыков, пар и троек в DuckDB по тем же данным из S3.

Читает `src/sql/skills_counts.sql`, подставляет параметры (S3-URI, условие
варианта из общего модуля, пути вывода) и выполняет запросы. Результаты
пишутся локально и заливаются в хранилище: results/duckdb/*.csv.

Запуск:
    python scripts/run_duckdb.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb  # noqa: E402

from common.config import load_settings  # noqa: E402
from common.duckdb import connect, render, run_statements  # noqa: E402
from common.reporting import Transcript, human  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402
from common.variant import VARIANT, filter_predicate_sql  # noqa: E402

SQL_FILE = REPO_ROOT / "src" / "sql" / "skills_counts.sql"


def main() -> int:
    settings = load_settings()
    out_dir = Path(settings.output_dir) / "duckdb"
    out_dir.mkdir(parents=True, exist_ok=True)
    transcript = Path(settings.report_dir) / "stage3_duckdb.txt"
    client = make_s3_client(settings)

    out_paths = {
        "OUT_K1": out_dir / "skills_k1.csv",
        "OUT_K2": out_dir / "skills_k2.csv",
        "OUT_K3": out_dir / "skills_k3.csv",
        "OUT_HIST": out_dir / "skill_histogram.csv",
        "OUT_SUMMARY": out_dir / "summary.csv",
    }
    sql_values = {
        "INPUT_URI": settings.input_uri,
        "VARIANT_PREDICATE": filter_predicate_sql("v"),
        **{name: path.as_posix() for name, path in out_paths.items()},
    }

    template = SQL_FILE.read_text(encoding="utf-8")
    sql = render(template, sql_values)

    started_all = time.perf_counter()
    with Transcript(transcript) as t:
        t.rule("ЭТАП 3б. DuckDB: частоты навыков, пар и троек (данные из S3)")
        t.log()
        t.log(f"  вариант     : {VARIANT['number']} — {VARIANT['title']}")
        t.log(f"  правило     : {filter_predicate_sql('v')}")
        t.log(f"  объект      : {settings.input_uri}")
        t.log(f"  SQL         : {SQL_FILE.relative_to(REPO_ROOT)}")
        t.log(f"  DuckDB      : {duckdb.__version__}, "
              f"memory_limit={settings.duckdb_memory_limit}, "
              f"threads={settings.duckdb_threads}")
        t.log()

        con = connect(settings)
        t.log("  [OK] httpfs загружен, S3 настроен (path-style, без SSL)")
        t.log()

        # duckdb.extract_statements корректно режет и по ';' внутри комментариев.
        t.rule("1. Выполнение SQL (по инструкциям)")
        run_statements(con, sql, lambda i, sec, first: t.log(
            f"  [{i:02d}] {sec:6.2f} с  {first[:90]}"))
        t.log(f"  всего SQL: {time.perf_counter() - started_all:.1f} с")
        t.log()

        t.rule("2. Контроль выборки")
        total_vacancies = con.execute("SELECT count(*) FROM variant").fetchone()[0]
        with_skills = con.execute(
            "SELECT count(DISTINCT vacancy_id) FROM vacancy_skill"
        ).fetchone()[0]
        pairs = con.execute("SELECT count(*) FROM vacancy_skill").fetchone()[0]
        t.log(f"  вакансий варианта          : {total_vacancies}")
        t.log(f"  из них с навыками          : {with_skills}")
        t.log(f"  без навыков                : {total_vacancies - with_skills}")
        t.log(f"  пар (vacancy_id, skill)    : {pairs}")
        t.log()

        t.rule("3. Контрольные числа (должны совпасть с MapReduce)")
        t.log(f"  {'k':>2} {'уникальных ключей':>18} {'вхождений':>12} {'ожидается из гистограммы':>26}")
        for k, unique, total, expected in con.execute(
            "SELECT k, unique_keys, total_occurrences, expected_from_histogram "
            "FROM summary ORDER BY k"
        ).fetchall():
            t.log(f"  {k:>2} {unique:>18} {total:>12} {expected:>26}")
        t.log()

        t.rule("4. ТОП-10 ключей")
        tops = {
            1: ("SELECT skill1, cnt FROM skills_k1 ORDER BY cnt DESC, skill1 LIMIT 10", 1),
            2: ("SELECT skill1, skill2, cnt FROM skills_k2 ORDER BY cnt DESC, skill1, skill2 LIMIT 10", 2),
            3: ("SELECT skill1, skill2, skill3, cnt FROM skills_k3 ORDER BY cnt DESC, skill1, skill2, skill3 LIMIT 10", 3),
        }
        for k, (query, width) in tops.items():
            t.log(f"  k={k}:")
            for row in con.execute(query).fetchall():
                key = " + ".join(row[:width])
                t.log(f"    {key:<70} {row[-1]:>6}")
            t.log()

        t.rule("5. Запись результатов и загрузка в хранилище")
        for name, path in out_paths.items():
            t.log(f"  {name:<10} {path}  ({human(path.stat().st_size)})")
            key = f"{settings.s3_prefix_results}duckdb/{path.name}"
            client.put_object(
                Bucket=settings.s3_bucket,
                Key=key,
                Body=path.read_bytes(),
                ContentType="text/csv",
            )
            obj = client.head_object(Bucket=settings.s3_bucket, Key=key)
            t.log(f"    [OK] s3://{settings.s3_bucket}/{key} "
                  f"({human(obj['ContentLength'])})")
        t.log()

        t.rule("ИТОГ ЭТАПА 3б")
        t.log(f"  время: {time.perf_counter() - started_all:.1f} с")
        t.log(f"  транскрипт: {transcript}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
