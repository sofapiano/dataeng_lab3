"""Этап 5. Влияние навыков на зарплату: пары и тройки навыков.

Считает, насколько пары и тройки навыков повышают или понижают медианную
зарплату вакансии относительно базового (более частого) навыка комбинации.

Данные читаются напрямую из объектного хранилища (s3://...), тот же CSV и то
же правило отбора варианта, что на этапах 3-4. Расчётные запросы лежат в
`src/sql/salary_analysis.sql`.

Запуск:
    python scripts/salary_analysis.py
    python scripts/salary_analysis.py --min-pair-n 50 --min-triple-n 30
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb  # noqa: E402

from common.config import load_settings  # noqa: E402
from common.duckdb import connect, render  # noqa: E402
from common.reporting import Transcript  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402
from common.variant import VARIANT, filter_predicate_sql  # noqa: E402

SQL_FILE = REPO_ROOT / "src" / "sql" / "salary_analysis.sql"

#: Коэффициент перевода gross -> net (НДФЛ 13 %).
GROSS_TO_NET = 0.87
#: Границы допустимой зарплаты, руб./мес: отсекаются явные выбросы (100, 1500).
SALARY_MIN = 10_000
SALARY_MAX = 1_000_000

#: Лимит памяти DuckDB для этого этапа. Рабочее множество крошечное (4 тыс.
#: вакансий), а вот CSV-скан по объекту в 808 МБ буферизуется целиком — на
#: машине со свободными ~0.6 ГБ лимит из конфигурации (1 ГБ) приводит
#: к «Allocation failure», поэтому здесь он меньше.
MEMORY_LIMIT = "256MB"

#: Один поток: параллельный CSV-скан по httpfs держит в памяти буфер на
#: каждый поток, а на этой машине памяти мало.
THREADS = 1


def human(n: float) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "Б" else f"{int(n)} Б"
        n /= 1024
    return f"{n:.1f} ТБ"


def money(value: float | None) -> str:
    """Денежное значение с пробелами-разделителями тысяч."""
    if value is None:
        return "-"
    return f"{value:,.0f}".replace(",", " ")


def pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.1f}%"


def print_table(t: Transcript, headers: list[str], rows: list[list[str]]) -> None:
    """Печатает таблицу с выравниванием колонок по ширине содержимого."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    t.log("  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    t.log("  " + "  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rows:
        t.log("  " + "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))


def effect_rows(rows: list[tuple], width: int) -> list[list[str]]:
    """Приводит строки *_effects к виду для печати."""
    out: list[list[str]] = []
    for row in rows:
        combo = " + ".join(row[:width])
        n, med_combo, base, n_base, med_base, delta, delta_pct, jr_combo, jr_base = row[width:]
        out.append([
            combo,
            str(n),
            money(med_combo),
            f"{base} (n={n_base})",
            money(med_base),
            money(delta),
            pct(delta_pct),
            f"{jr_combo * 100:.0f}%" if jr_combo is not None else "-",
            f"{jr_base * 100:.0f}%" if jr_base is not None else "-",
        ])
    return out


def junior_verdict(combo: float | None, base: float | None, sign: int) -> str:
    """Формулирует вывод о том, объясняется ли эффект составом вакансий.

    sign = 1 для повышающих комбинаций, -1 для понижающих.
    """
    if combo is None or base is None:
        return "нет данных"
    diff = (combo - base) * 100
    if sign > 0:
        if diff <= -5:
            return "составом джунов не объясняется (комбинация даже опытнее базы)"
        if diff >= 5:
            return "частично объясняется составом (в комбинации больше джунов)"
        return "доли сопоставимы — вероятнее связь с самим навыком"
    if diff >= 5:
        return "частично объясняется составом (в комбинации больше джунов)"
    if diff <= -5:
        return "составом джунов не объясняется (комбинация даже опытнее базы)"
    return "доли сопоставимы — вероятнее связь с самим навыком"


def check_stage3_consistency(settings, skills_in_variant: int, t: Transcript) -> None:
    """Сверяет число навыков варианта с результатом этапа 3 (k=1, MapReduce)."""
    path = Path(settings.output_dir) / "mr" / "summary.csv"
    if not path.is_file():
        t.log("  [i] results/mr/summary.csv не найден — сверка с этапом 3 пропущена")
        return
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        parts = line.split(",")
        if parts and parts[0] == "1":
            expected = int(parts[1])
            t.log(f"  контроль этапа 3: навыков в варианте {skills_in_variant} "
                  f"({'==' if expected == skills_in_variant else '!='} {expected} в MapReduce, k=1)")
            return


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()
    client = make_s3_client(settings)
    out_dir = Path(settings.output_dir) / "salary"
    out_dir.mkdir(parents=True, exist_ok=True)
    transcript = Path(args.out) if args.out else Path(settings.report_dir) / "stage5_salary.txt"

    out_paths = {
        "OUT_SUMMARY": out_dir / "summary.csv",
        "OUT_SKILLS": out_dir / "skill_effects.csv",
        "OUT_PAIRS": out_dir / "pair_effects.csv",
        "OUT_TRIPLES": out_dir / "triple_effects.csv",
        "OUT_PAIRS_UP": out_dir / "top_pairs_up.csv",
        "OUT_PAIRS_DOWN": out_dir / "top_pairs_down.csv",
        "OUT_TRIPLES_UP": out_dir / "top_triples_up.csv",
        "OUT_TRIPLES_DOWN": out_dir / "top_triples_down.csv",
    }
    sql_values = {
        "INPUT_URI": settings.input_uri,
        "VARIANT_PREDICATE": filter_predicate_sql("v"),
        "GROSS_TO_NET": str(GROSS_TO_NET),
        "SALARY_MIN": str(SALARY_MIN),
        "SALARY_MAX": str(SALARY_MAX),
        "MIN_PAIR_N": str(args.min_pair_n),
        "MIN_TRIPLE_N": str(args.min_triple_n),
        "MIN_BASE_N": str(args.min_base_n),
        "TOP": str(args.top),
        **{name: path.as_posix() for name, path in out_paths.items()},
    }
    sql = render(SQL_FILE.read_text(encoding="utf-8"), sql_values)

    started_all = time.perf_counter()
    with Transcript(transcript) as t:
        t.rule("ЭТАП 5. ВЛИЯНИЕ НАВЫКОВ НА ЗАРАБОТНУЮ ПЛАТУ")
        t.log()
        t.log(f"  вариант      : {VARIANT['number']} — {VARIANT['title']}")
        t.log(f"  правило      : {filter_predicate_sql('v')}")
        t.log(f"  объект       : {settings.input_uri}")
        t.log(f"  SQL          : {SQL_FILE.relative_to(REPO_ROOT)}")
        t.log(f"  DuckDB       : {duckdb.__version__}, "
              f"memory_limit={args.memory_limit}, threads={args.threads}")
        t.log()
        t.log("  Модель зарплаты:")
        t.log("    * только RUR; gross -> net (x0.87); середина вилки, иначе единственная граница;")
        t.log(f"    * отбрасываются выбросы вне [{SALARY_MIN:,} .. {SALARY_MAX:,}] руб./мес".replace(",", " "))
        t.log("  Методика прибавки:")
        t.log("    базовый навык комбинации — самый популярный её участник (во всём варианте);")
        t.log("    прибавка = медиана(комбинация) - медиана(базовый навык);")
        t.log(f"    пороги выборки: пара n >= {args.min_pair_n}, тройка n >= {args.min_triple_n}, "
              f"базовый навык n >= {args.min_base_n}")
        t.log()

        con = connect(settings, memory_limit=args.memory_limit, threads=args.threads)
        t.rule("1. Выполнение SQL")
        for i, statement in enumerate(con.extract_statements(sql), 1):
            query = statement.query
            first_line = next(
                (ln.strip() for ln in query.splitlines()
                 if ln.strip() and not ln.strip().startswith("--")),
                "",
            )
            started = time.perf_counter()
            con.execute(query)
            t.log(f"  [{i:02d}] {time.perf_counter() - started:6.2f} с  {first_line[:88]}")
        t.log()

        t.rule("2. Воронка данных")
        print_table(
            t,
            ["Шаг", "Вакансий"],
            [[step, str(n)] for _, step, n in con.execute(
                "SELECT step_order, step, n FROM summary ORDER BY step_order").fetchall()],
        )
        t.log()
        skills_in_variant = con.execute("SELECT count(*) FROM skill_pop").fetchone()[0]
        check_stage3_consistency(settings, skills_in_variant, t)
        t.log()

        t.rule("3. Рабочая выборка")
        n, junior_share, gross_share, one_bound, two_bounds = con.execute(
            "SELECT n, junior_share, gross_share, one_bound, two_bounds FROM sample_stats"
        ).fetchone()
        med_all, p25, p75, mn, mx = con.execute(
            "SELECT med_all, p25, p75, mn, mx FROM salary_baseline"
        ).fetchone()
        t.log(f"  вакансий с валидной зарплатой : {n}")
        t.log(f"  медиана зарплаты              : {money(med_all)} руб.")
        t.log(f"  p25 / p75                     : {money(p25)} / {money(p75)} руб.")
        t.log(f"  min / max                     : {money(mn)} / {money(mx)} руб.")
        t.log(f"  доля gross (переведено в net) : {gross_share * 100:.1f}%")
        t.log(f"  вилка «от и до» / одна граница: {two_bounds} / {one_bound}")
        t.log(f"  доля вакансий с опытом < 3 лет: {junior_share * 100:.1f}%")
        t.log()

        t.rule("4. Отдельные навыки: отклонение медианы от медианы выборки")
        t.log(f"  (порог n >= {args.min_base_n}; база — медиана рабочей выборки {money(med_all)} руб.)")
        skill_headers = ["Навык", "n (вариант)", "n (зарплата)", "Медиана", "Δ, руб.", "Δ, %", "P25", "P75"]
        skill_rows = [
            [skill, str(n_all), str(n_sal), money(med), money(delta), pct(delta_pct),
             money(p25), money(p75)]
            for skill, n_all, n_sal, med, delta, delta_pct, p25, p75 in con.execute(
                """
                SELECT skill, n_all, n_sal, med_sal, delta, delta_pct, p25_sal, p75_sal
                FROM skill_effects
                ORDER BY delta DESC, skill
                """
            ).fetchall()[: args.top]
        ]
        t.log("  ТОП по медиане:")
        print_table(t, skill_headers, skill_rows)
        t.log()
        skill_rows = [
            [skill, str(n_all), str(n_sal), money(med), money(delta), pct(delta_pct),
             money(p25), money(p75)]
            for skill, n_all, n_sal, med, delta, delta_pct, p25, p75 in con.execute(
                """
                SELECT skill, n_all, n_sal, med_sal, delta, delta_pct, p25_sal, p75_sal
                FROM skill_effects
                ORDER BY delta ASC, skill
                """
            ).fetchall()[: args.top]
        ]
        t.log("  НИЗ по медиане:")
        print_table(t, skill_headers, skill_rows)
        t.log()

        effect_headers = ["Комбинация", "n", "Медиана комб.", "Базовый навык",
                          "Медиана базы", "Δ, руб.", "Δ, %", "Junior комб.", "Junior базы"]
        for title, table, width, order in (
            ("5. ПАРЫ НАВЫКОВ: топ по прибавке", "pair_effects", 2, "DESC"),
            ("6. ПАРЫ НАВЫКОВ: топ по снижению", "pair_effects", 2, "ASC"),
            ("7. ТРОЙКИ НАВЫКОВ: топ по прибавке", "triple_effects", 3, "DESC"),
            ("8. ТРОЙКИ НАВЫКОВ: топ по снижению", "triple_effects", 3, "ASC"),
        ):
            t.rule(title)
            rows = con.execute(
                f"SELECT * FROM {table} ORDER BY delta {order} LIMIT {args.top}"
            ).fetchall()
            print_table(t, effect_headers, effect_rows(rows, width))
            t.log()

        t.rule("9. Наблюдения по числам выше")
        best = con.execute(
            "SELECT skill1, skill2, n, med_combo, base_skill, med_base, delta "
            "FROM pair_effects ORDER BY delta DESC LIMIT 1"
        ).fetchone()
        worst = con.execute(
            "SELECT skill1, skill2, n, med_combo, base_skill, med_base, delta "
            "FROM pair_effects ORDER BY delta ASC LIMIT 1"
        ).fetchone()
        best_triple = con.execute(
            "SELECT skill1, skill2, skill3, n, med_combo, base_skill, med_base, delta "
            "FROM triple_effects ORDER BY delta DESC LIMIT 1"
        ).fetchone()
        worst_triple = con.execute(
            "SELECT skill1, skill2, skill3, n, med_combo, base_skill, med_base, delta "
            "FROM triple_effects ORDER BY delta ASC LIMIT 1"
        ).fetchone()
        t.log(f"  Медиана рабочей выборки: {money(med_all)} руб., "
              f"доля вакансий с опытом < 3 лет: {junior_share * 100:.1f}%.")
        t.log(f"  Лучшая пара   : {best[0]} + {best[1]} (n={best[2]}) -> {money(best[3])} руб., "
              f"база {best[4]} = {money(best[5])} руб., прибавка {money(best[6])} руб.")
        t.log(f"  Худшая пара   : {worst[0]} + {worst[1]} (n={worst[2]}) -> {money(worst[3])} руб., "
              f"база {worst[4]} = {money(worst[5])} руб., прибавка {money(worst[6])} руб.")
        t.log(f"  Лучшая тройка : {' + '.join(best_triple[:3])} (n={best_triple[3]}) -> "
              f"{money(best_triple[4])} руб., база {best_triple[5]} = {money(best_triple[6])} руб., "
              f"прибавка {money(best_triple[7])} руб.")
        t.log(f"  Худшая тройка : {' + '.join(worst_triple[:3])} (n={worst_triple[3]}) -> "
              f"{money(worst_triple[4])} руб., база {worst_triple[5]} = {money(worst_triple[6])} руб., "
              f"прибавка {money(worst_triple[7])} руб.")
        t.log()

        t.log("  Состав вакансий (доля с опытом < 3 лет): комбинация против базы")
        for label, table in (("пар", "pair_effects"), ("троек", "triple_effects")):
            for direction, order, sign in (("повышение", "DESC", 1), ("снижение", "ASC", -1)):
                combo, base = con.execute(
                    f"SELECT avg(junior_combo), avg(junior_base) FROM "
                    f"(SELECT * FROM {table} ORDER BY delta {order} LIMIT {args.top})"
                ).fetchone()
                t.log(f"    топ-{args.top} {label} ({direction:<8}): комбинация {combo * 100:.0f}%, "
                      f"база {base * 100:.0f}%  ->  {junior_verdict(combo, base, sign)}")
        t.log()

        t.rule("10. Устойчивость ранга к порогу выборки")
        t.log("  Топ-3 комбинации при более строгом пороге n (отсекает шумные медианы):")
        for label, table, width, strict in (
            ("пары", "pair_effects", 2, max(50, args.min_pair_n)),
            ("тройки", "triple_effects", 3, max(30, args.min_triple_n)),
        ):
            total = con.execute(f"SELECT count(*) FROM {table} WHERE n >= {strict}").fetchone()[0]
            for direction, order in (("повышение", "DESC"), ("снижение", "ASC")):
                rows = con.execute(
                    f"SELECT * FROM {table} WHERE n >= {strict} ORDER BY delta {order} LIMIT 3"
                ).fetchall()
                names = "; ".join(
                    f"{' + '.join(r[:width])} (n={r[width]}, {money(r[width + 5])})" for r in rows
                )
                t.log(f"  {label} (n >= {strict}, всего {total} комбинаций), {direction}: {names}")
        t.log()

        t.rule("11. Запись результатов и загрузка в хранилище")
        for name, path in out_paths.items():
            t.log(f"  {name:<18} {path}  ({human(path.stat().st_size)})")
        if not args.no_upload:
            for name, path in out_paths.items():
                key = f"{settings.s3_prefix_results}salary/{path.name}"
                client.put_object(
                    Bucket=settings.s3_bucket,
                    Key=key,
                    Body=path.read_bytes(),
                    ContentType="text/csv",
                )
                obj = client.head_object(Bucket=settings.s3_bucket, Key=key)
                t.log(f"    [OK] s3://{settings.s3_bucket}/{key} ({human(obj['ContentLength'])})")
        t.log()

        t.rule("ИТОГ ЭТАПА 5")
        t.log(f"  время: {time.perf_counter() - started_all:.1f} с")
        t.log(f"  транскрипт: {transcript}")

    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Этап 5: влияние навыков на зарплату")
    parser.add_argument("--min-pair-n", type=int, default=30,
                        help="минимальное число вакансий в паре")
    parser.add_argument("--min-triple-n", type=int, default=20,
                        help="минимальное число вакансий в тройке")
    parser.add_argument("--min-base-n", type=int, default=30,
                        help="минимальное число вакансий у базового навыка")
    parser.add_argument("--top", type=int, default=10, help="размер итоговых таблиц")
    parser.add_argument("--memory-limit", default=MEMORY_LIMIT,
                        help="лимит памяти DuckDB (по умолчанию меньше конфигурационного: "
                             "CSV-скан по объекту в 808 МБ буферизуется целиком)")
    parser.add_argument("--threads", type=int, default=THREADS,
                        help="потоки DuckDB")
    parser.add_argument("--out", default=None, help="файл транскрипта")
    parser.add_argument("--no-upload", action="store_true", help="не заливать результаты в S3")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
