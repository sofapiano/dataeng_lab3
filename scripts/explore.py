import sys, os, re
import duckdb
import pandas as pd

path = (sys.argv[1] if len(sys.argv) > 1 else "data/raw/it_vacancies_2024.csv").replace("\\", "/")
os.makedirs("out", exist_ok=True)
rep = open("out/report.txt", "w", encoding="utf-8")

def log(*a):
    s = " ".join(str(x) for x in a)
    print(s)
    rep.write(s + "\n")
    rep.flush()

con = duckdb.connect("out/stage0.duckdb")
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 50)

def show(title, sql, maxw=70):
    log(f"\n=== {title} ===")
    log(sql.strip())
    df = con.execute(sql).fetchdf()
    log(df.to_string(max_colwidth=maxw, index=False))
    return df

show("sniff_csv", f"SELECT * FROM sniff_csv('{path}')", maxw=120)

# Загружаем всё как текст чтобы ничего не потерялось при приведении типов
con.execute(f"""
    CREATE TABLE IF NOT EXISTS v AS
    SELECT * FROM read_csv('{path}', header=true, all_varchar=true, sample_size=-1)
""")

cols = [r[0] for r in con.execute("DESCRIBE v").fetchall()]
n_rows = con.execute("SELECT count(*) FROM v").fetchone()[0]
log(f"\n=== Колонки ({len(cols)}) и число строк ===")
log(f"rows = {n_rows}")
log(", ".join(cols))

# проверка переноса строк внутри полей (критично для чанков MapReduce)
nl = 0
with open(path, "rb") as f:
    while chunk := f.read(1 << 24):
        nl += chunk.count(b"\n")
log(f"\n=== Проверка переносов строк ===")
log(f"символов \\n в файле: {nl}, записей (строк данных): {n_rows}")
log("Если \\n ≈ rows+1 -> переносов внутри полей нет, можно резать по строкам."
    " Если \\n заметно больше -> есть многострочные поля, нужен CSV-парсер.")

show("Первые 5 строк", "SELECT * FROM v LIMIT 5", maxw=40)

# доля пустых значений
exprs = ", ".join(
    f"count(*) FILTER (WHERE \"{c}\" IS NULL OR trim(\"{c}\")='') AS c{i}"
    for i, c in enumerate(cols))
row = con.execute(f"SELECT {exprs} FROM v").fetchone()
log("\n=== Пропуски по колонкам ===")
for c, k in zip(cols, row):
    log(f"{c:40s} пустых: {k:>9d}  ({100*k/n_rows:5.1f}%)")

# колонки с малым числом значений (кандидаты на сегмент/валюту/gross и т.п.)
dexprs = ", ".join(f"approx_count_distinct(\"{c}\") AS c{i}" for i, c in enumerate(cols))
drow = con.execute(f"SELECT {dexprs} FROM v").fetchone()
log("\n=== Число различных значений ===")
for c, d in zip(cols, drow):
    log(f"{c:40s} ~{d}")
for c, d in zip(cols, drow):
    if d <= 40:
        show(f"Значения колонки {c}",
             f'SELECT "{c}" AS value, count(*) AS n FROM v GROUP BY 1 ORDER BY n DESC LIMIT 40')

# колонки по ключевым словам в названии
def find(pattern):
    return [c for c in cols if re.search(pattern, c, re.I)]

groups = {
    "ID": r"(^id$|_id$|^id_)",
    "НАЗВАНИЕ": r"name|title|должн|vacancy|профес|position",
    "НАВЫКИ": r"skill|навык|tag|стек|stack",
    "ЗАРПЛАТА": r"salary|зарп|оклад|pay|from|to$|currency|gross|rub",
    "РЕГИОН": r"area|region|регион|город|city|location",
}
log("\n=== Кандидаты в колонки ===")
for g, p in groups.items():
    log(f"{g}: {find(p)}")

for g in ["НАЗВАНИЕ", "НАВЫКИ", "ЗАРПЛАТА", "РЕГИОН"]:
    for c in find(groups[g]):
        show(f"[{g}] примеры значений: {c}",
             f"""SELECT "{c}" AS value FROM v
                 WHERE "{c}" IS NOT NULL AND trim("{c}")<>'' USING SAMPLE 10 ROWS""", maxw=120)

# формат навыков: какие разделители встречаются
for c in find(groups["НАВЫКИ"]):
    show(f"Разделители в {c} (число значений с символом)",
         f"""SELECT
                count(*) FILTER (WHERE "{c}" LIKE '%,%')  AS comma,
                count(*) FILTER (WHERE "{c}" LIKE '%;%')  AS semicolon,
                count(*) FILTER (WHERE "{c}" LIKE '%|%')  AS pipe,
                count(*) FILTER (WHERE "{c}" LIKE '[%')   AS bracket_list,
                count(*) FILTER (WHERE "{c}" LIKE '%' || chr(10) || '%') AS newline,
                count(*) FILTER (WHERE "{c}" IS NOT NULL AND trim("{c}")<>'') AS non_empty
             FROM v""")

# числовые колонки зарплаты: диапазоны
for c in find(r"salary|зарп|оклад|from|to$"):
    show(f"Числовой профиль: {c}",
         f"""SELECT count(try_cast("{c}" AS DOUBLE)) AS numeric_cnt,
                    min(try_cast("{c}" AS DOUBLE)) AS min,
                    median(try_cast("{c}" AS DOUBLE)) AS median,
                    max(try_cast("{c}" AS DOUBLE)) AS max
             FROM v""")

# топ регионов
for c in find(groups["РЕГИОН"]):
    show(f"Топ-15 значений: {c}",
         f'SELECT "{c}" AS value, count(*) AS n FROM v GROUP BY 1 ORDER BY n DESC LIMIT 15')

# уникальность id
for c in find(groups["ID"]):
    show(f"Уникальность {c}",
         f'SELECT count(*) AS total, count(DISTINCT "{c}") AS uniq FROM v')

log("\nГотово. Отчёт: out/report.txt")