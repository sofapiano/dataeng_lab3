import os
import duckdb

os.makedirs("out", exist_ok=True)
rep = open("out/variant_filter_check.txt", "w", encoding="utf-8")

def log(s=""):
    print(s)
    rep.write(s + "\n")
    rep.flush()

con = duckdb.connect("out/stage0.duckdb", read_only=True)

MOBILE = (r"(^|[^a-zа-я0-9])(ios|android|swift|kotlin|flutter|react native|mobile|андроид)"
          r"([^a-zа-я0-9]|$)|мобильн")
ROLE = "Программист, разработчик"

def run(title, sql):
    log(f"\n=== {title} ===")
    df = con.execute(sql).fetchdf()
    log(df.to_string(index=False, max_colwidth=90))

run("1. Вакансии сегмента (role + слова в названии)", f"""
    SELECT count(*) AS n_mobile_by_name
    FROM v
    WHERE role = '{ROLE}' AND regexp_matches(lower(name), '{MOBILE}')""")

run("2. Мобильные названия по всем ролям", f"""
    SELECT role, count(*) AS n FROM v
    WHERE regexp_matches(lower(name), '{MOBILE}')
    GROUP BY role ORDER BY n DESC""")

run("3. 30 случайных названий из сегмента", f"""
    SELECT name FROM v
    WHERE role = '{ROLE}' AND regexp_matches(lower(name), '{MOBILE}')
    USING SAMPLE 30 ROWS""")

run("4. Навыки и зарплата в сегменте", f"""
    SELECT count(*) AS total,
           count(*) FILTER (WHERE key_skills IS NOT NULL AND trim(key_skills) <> '') AS with_skills,
           count(*) FILTER (WHERE currency = 'RUR'
                            AND (salary_from IS NOT NULL OR salary_to IS NOT NULL)) AS with_salary_rur
    FROM v
    WHERE role = '{ROLE}' AND regexp_matches(lower(name), '{MOBILE}')""")

run("5. Возможные пропуски: названия в роли с похожими словами, но вне фильтра", f"""
    SELECT name FROM v
    WHERE role = '{ROLE}'
      AND NOT regexp_matches(lower(name), '{MOBILE}')
      AND regexp_matches(lower(name), 'objective|xamarin|kmm|react-native|ios|андроид|мобил|app|прилож')
    USING SAMPLE 30 ROWS""")

log("\nГотово: out/variant_filter_check.txt")