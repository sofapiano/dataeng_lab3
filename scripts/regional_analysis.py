"""Этап 6. Региональный анализ: три крупнейших региона варианта.

Сравнивает регионы по зарплатам и востребованности навыков (одиночных, пар и
троек). Регионы выбираются по числу вакансий варианта — так в каждом хватает
данных на медианы и навыки.

Данные читаются напрямую из объектного хранилища (s3://...), тот же CSV и то
же правило отбора варианта, что на этапах 3-5. Модель зарплаты и нормализация
навыков — общие с этапом 5 (`src/sql/vacancy_model.sql`), запросы этапа —
`src/sql/regional_analysis.sql`.

Запуск:
    python scripts/regional_analysis.py
    python scripts/regional_analysis.py --regions 5 --top 5
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb  # noqa: E402

from common import salary as salary_model  # noqa: E402
from common.config import load_settings  # noqa: E402
from common.duckdb import connect, render, run_statements  # noqa: E402
from common.reporting import Transcript, human, money  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402
from common.variant import VARIANT, filter_predicate_sql  # noqa: E402

SQL_FILE = REPO_ROOT / "src" / "sql" / "regional_analysis.sql"

#: Общая с этапом 5 часть SQL: vacancy, vacancy_skill, skill_pop.
MODEL_FILE = REPO_ROOT / "src" / "sql" / "vacancy_model.sql"

#: Лимит памяти DuckDB для этого этапа: CSV-скан по объекту в 808 МБ
#: буферизуется целиком, а на машине свободно ~0.6 ГБ (см. этап 5).
MEMORY_LIMIT = "256MB"
THREADS = 1

#: Сколько регионов сравниваем; с какого n в регионе и во всём варианте считаем
#: индекс востребованности (на редких навыках он шумит: ×20 при n=5).
REGION_LIMIT = 3
MIN_REGION_SKILL_N = 10
MIN_SKILL_TOTAL_N = 30

#: Порядок групп опыта в таблице «медиана по региону и опыту».
EXPERIENCE_ORDER = [
    "Нет опыта",
    "От 1 года до 3 лет",
    "От 3 до 6 лет",
    "Более 6 лет",
    "—",
]


def share(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.0f}%"


def signed_money(value: float) -> str:
    """Сумма со знаком: +45 000 руб. / -12 000 руб."""
    return ("+" if value > 0 else "") + money(value) + " руб."


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()
    client = make_s3_client(settings)
    out_dir = Path(settings.output_dir) / "regions"
    out_dir.mkdir(parents=True, exist_ok=True)
    transcript = Path(args.out) if args.out else Path(settings.report_dir) / "stage6_regions.txt"

    out_paths = {
        "OUT_REGIONS": out_dir / "region_salary.csv",
        "OUT_SKILLS_TOP": out_dir / "region_skills_top.csv",
        "OUT_SKILLS_INDEX": out_dir / "region_skills_index.csv",
        "OUT_PAIRS": out_dir / "region_pairs.csv",
        "OUT_TRIPLES": out_dir / "region_triples.csv",
        "OUT_EXPERIENCE": out_dir / "region_experience.csv",
    }
    sql_values = {
        "INPUT_URI": settings.input_uri,
        "VARIANT_PREDICATE": filter_predicate_sql("v"),
        **salary_model.sql_values(),
        "REGION_LIMIT": str(args.regions),
        "MIN_REGION_SKILL_N": str(MIN_REGION_SKILL_N),
        "MIN_SKILL_TOTAL_N": str(MIN_SKILL_TOTAL_N),
        "TOP": str(args.top),
        **{name: path.as_posix() for name, path in out_paths.items()},
    }
    model = MODEL_FILE.read_text(encoding="utf-8")
    sql = render(model + "\n" + SQL_FILE.read_text(encoding="utf-8"), sql_values)

    started_all = time.perf_counter()
    with Transcript(transcript) as t:
        t.rule("ЭТАП 6. РЕГИОНАЛЬНЫЙ АНАЛИЗ")
        t.log()
        t.log(f"  вариант      : {VARIANT['number']} — {VARIANT['title']}")
        t.log(f"  правило      : {filter_predicate_sql('v')}")
        t.log(f"  объект       : {settings.input_uri}")
        t.log(f"  SQL          : {SQL_FILE.relative_to(REPO_ROOT)}")
        t.log(f"  модель       : {MODEL_FILE.relative_to(REPO_ROOT)} (общая с этапом 5)")
        t.log(f"  DuckDB       : {duckdb.__version__}, "
              f"memory_limit={args.memory_limit}, threads={args.threads}")
        t.log(f"  регионы      : top-{args.regions} по числу вакансий варианта")
        t.log()

        con = connect(settings, memory_limit=args.memory_limit, threads=args.threads)
        t.rule("1. Выполнение SQL")
        run_statements(con, sql, lambda i, sec, first: t.log(
            f"  [{i:02d}] {sec:6.2f} с  {first[:88]}"))
        t.log()

        t.rule("2. Регионы варианта (контекст выбора)")
        total = con.execute("SELECT count(*) FROM vacancy").fetchone()[0]
        chosen = {row[0] for row in con.execute("SELECT area FROM region").fetchall()}
        t.table(
            ["Регион", "Вакансий", "Доля варианта", "Сравниваем"],
            [
                [area, str(n), share(n / total), "да" if area in chosen else ""]
                for area, n in con.execute(
                    "SELECT area, n_vac FROM area_counts ORDER BY n_vac DESC, area LIMIT 10"
                ).fetchall()
            ],
        )
        t.log(f"  в варианте {total} вакансий, регионов с названием: "
              f"{con.execute('SELECT count(*) FROM area_counts').fetchone()[0]}")
        t.log()

        t.rule("3. Зарплаты по регионам")
        salary_rows = con.execute(
            """
            SELECT area, n_vac, n_with_skills, n_any_currency, n_valid,
                   salary_share, med, p25, p75, junior_share, gross_share
            FROM region_salary ORDER BY region_rank
            """
        ).fetchall()
        t.table(
            ["Регион", "Вакансий", "С навыками", "С зарплатой", "Доля с ЗП",
             "Медиана", "P25", "P75", "Джунов", "Gross"],
            [
                [area, str(n_vac), str(n_skills), str(n_any), share(salary_share),
                 money(med), money(p25), money(p75), share(junior), share(gross)]
                for area, n_vac, n_skills, n_any, n_valid,
                salary_share, med, p25, p75, junior, gross in salary_rows
            ],
        )
        t.log("  (медиана, P25, P75 — по валидной зарплате: RUR, без выбросов)")
        t.log()

        t.rule("4. Медиана по региону и опыту (одна ли картина внутри грейдов)")
        experience = con.execute(
            "SELECT area, experience, n, med FROM region_experience"
        ).fetchall()
        by_exp: dict[str, dict[str, tuple[int, float]]] = {}
        for area, exp, n, med in experience:
            by_exp.setdefault(exp, {})[area] = (n, med)
        areas = [row[0] for row in salary_rows]
        order = [e for e in EXPERIENCE_ORDER if e in by_exp]
        order += [e for e in by_exp if e not in order]
        t.table(
            ["Опыт"] + areas,
            [
                [exp] + [
                    f"{money(by_exp[exp][area][1])} ({by_exp[exp][area][0]})"
                    if area in by_exp[exp] else "-"
                    for area in areas
                ]
                for exp in order
            ],
        )
        t.log("  (медиана зарплаты, в скобках — число вакансий)")
        t.log()

        t.rule("5. Востребованность навыков по регионам")
        skills = con.execute(
            """
            SELECT area, skill, n, share_region, share_variant, demand_index
            FROM region_skill
            QUALIFY row_number() OVER (PARTITION BY area ORDER BY n DESC, skill) <= ?
            ORDER BY region_rank, n DESC, skill
            """,
            [args.top],
        ).fetchall()
        for area in areas:
            t.log(f"  {area}:")
            t.table(
                ["Навык", "n", "Доля в регионе", "Доля в варианте", "Индекс"],
                [
                    [skill, str(n), share(sr), share(sv), f"{idx:.2f}"]
                    for a, skill, n, sr, sv, idx in skills if a == area
                ],
            )
            t.log()

        t.rule(f"6. Специализация регионов (n >= {MIN_REGION_SKILL_N} в регионе, "
               f"n_all >= {MIN_SKILL_TOTAL_N} в варианте)")
        t.log("  Индекс = доля навыка в регионе / доля во всём варианте: >1 — навык")
        t.log("  встречается в регионе чаще среднего, <1 — реже.")
        for area in areas:
            t.table(
                ["Регион", "Навык", "n", "Доля в регионе", "Доля в варианте", "Индекс"],
                [
                    [area, skill, str(n), share(sr), share(sv), f"{idx:.2f}"]
                    for skill, n, sr, sv, idx in con.execute(
                        """
                        SELECT skill, n, share_region, share_variant, demand_index
                        FROM region_skill
                        WHERE area = ? AND n >= ? AND n_all >= ?
                        ORDER BY demand_index DESC, skill LIMIT ?
                        """,
                        [area, MIN_REGION_SKILL_N, MIN_SKILL_TOTAL_N, args.top],
                    ).fetchall()
                ],
            )
            t.log()

        t.rule("7. Пары и тройки навыков по регионам (частота)")
        for area in areas:
            t.log(f"  {area}:")
            for label, table, width, cols in (
                ("пары", "region_pair", 2, "skill1, skill2"),
                ("тройки", "region_triple", 3, "skill1, skill2, skill3"),
            ):
                rows = con.execute(
                    f"""
                    SELECT * EXCLUDE (region_rank, area)
                    FROM {table} WHERE area = ?
                    ORDER BY n DESC, {cols} LIMIT ?
                    """,
                    [area, args.top],
                ).fetchall()
                t.log(f"    {label}:")
                for row in rows:
                    t.log(f"      {' + '.join(row[:width]):<58} {row[-1]:>5}")
            t.log()

        t.rule("8. Наблюдения по числам выше")
        best = max(salary_rows, key=lambda r: r[6])
        worst = min(salary_rows, key=lambda r: r[6])
        gap = best[6] - worst[6]
        t.log(f"  Медиана зарплаты: {best[0]} — {money(best[6])} руб., "
              f"{worst[0]} — {money(worst[6])} руб.; разрыв {money(gap)} руб. "
              f"({gap / worst[6] * 100:.0f}% к меньшей).")
        t.log("  Доля вакансий с указанной зарплатой: " + ", ".join(
            f"{r[0]} — {share(r[5])}" for r in salary_rows) + ".")
        t.log("  Доля вакансий с навыками: " + ", ".join(
            f"{r[0]} — {share(r[2] / r[1])}" for r in salary_rows) + ".")
        t.log("  Доля вакансий с опытом < 3 лет: " + ", ".join(
            f"{r[0]} — {share(r[9])}" for r in salary_rows) + ".")
        for exp in ("Нет опыта", "От 1 года до 3 лет", "От 3 до 6 лет", "Более 6 лет"):
            present = [(area, *by_exp[exp][area]) for area in areas
                       if area in by_exp.get(exp, {}) and by_exp[exp][area][0] >= 10]
            if len(present) < 2:
                continue
            t.log(f"  «{exp}»: " + ", ".join(
                f"{area} {money(med)} (n={n})" for area, n, med in present) + ".")

        # Разложение общего разрыва на состав по опыту и уровень оплаты внутри
        # грейдов: если внутри общих групп разрыв заметно меньше общего, то
        # регионы различаются не оплатой, а долей джунов.
        for other in salary_rows[1:]:
            overall = best[6] - other[6]
            common = [
                (exp, by_exp[exp][best[0]], by_exp[exp][other[0]])
                for exp in order
                if best[0] in by_exp.get(exp, {}) and other[0] in by_exp.get(exp, {})
                and by_exp[exp][best[0]][0] >= 10 and by_exp[exp][other[0]][0] >= 10
            ]
            if not common:
                t.log(f"  {best[0]} vs {other[0]}: общий разрыв {money(overall)} руб., "
                      "общих групп опыта с n >= 10 нет.")
                continue
            t.log(f"  {best[0]} vs {other[0]}: общий разрыв {money(overall)} руб., "
                  "внутри общих групп опыта: " + "; ".join(
                      f"«{exp}» {signed_money(med_a - med_b)}"
                      for exp, (_, med_a), (_, med_b) in common))
            # Взвешиваем разрыв внутри групп по числу вакансий: так он сравним
            # с общим разрывом, в котором доли грейдов уже сложились.
            total_n = sum(n_a + n_b for _, (n_a, _), (n_b, _) in common)
            within = sum((med_a - med_b) * (n_a + n_b)
                         for _, (n_a, med_a), (n_b, med_b) in common) / total_n
            t.log(f"    внутри групп (взвешено по n): {signed_money(within)} "
                  f"({within / overall * 100:+.0f}% общего разрыва) — "
                  + ("в основном это разный состав по опыту"
                     if abs(within) < 0.5 * abs(overall)
                     else "разрыв сохраняется и внутри грейдов, дело не только в составе"))

        for area in areas:
            lead = con.execute(
                """
                SELECT skill, n, demand_index FROM region_skill
                WHERE area = ? AND n >= ? AND n_all >= ?
                ORDER BY demand_index DESC, skill LIMIT 3
                """,
                [area, MIN_REGION_SKILL_N, MIN_SKILL_TOTAL_N],
            ).fetchall()
            t.log(f"  {area}, самые «свои» навыки: " + ", ".join(
                f"{skill} (×{idx:.2f}, n={n})" for skill, n, idx in lead) + ".")
        t.log()

        t.rule("9. Запись результатов и загрузка в хранилище")
        for name, path in out_paths.items():
            t.log(f"  {name:<18} {path}  ({human(path.stat().st_size)})")
        if not args.no_upload:
            for path in out_paths.values():
                key = f"{settings.s3_prefix_results}regions/{path.name}"
                client.put_object(
                    Bucket=settings.s3_bucket,
                    Key=key,
                    Body=path.read_bytes(),
                    ContentType="text/csv",
                )
                obj = client.head_object(Bucket=settings.s3_bucket, Key=key)
                t.log(f"    [OK] s3://{settings.s3_bucket}/{key} ({human(obj['ContentLength'])})")
        t.log()

        t.rule("ИТОГ ЭТАПА 6")
        t.log(f"  время: {time.perf_counter() - started_all:.1f} с")
        t.log(f"  транскрипт: {transcript}")

    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Этап 6: региональный анализ")
    parser.add_argument("--regions", type=int, default=REGION_LIMIT,
                        help="сколько регионов сравнивать (самые крупные по числу вакансий)")
    parser.add_argument("--top", type=int, default=10, help="размер таблиц и выгрузок")
    parser.add_argument("--memory-limit", default=MEMORY_LIMIT,
                        help="лимит памяти DuckDB (CSV-скан по объекту в 808 МБ "
                             "буферизуется целиком)")
    parser.add_argument("--threads", type=int, default=THREADS, help="потоки DuckDB")
    parser.add_argument("--out", default=None, help="файл транскрипта")
    parser.add_argument("--no-upload", action="store_true", help="не заливать результаты в S3")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
