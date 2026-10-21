import duckdb

con = duckdb.connect("out/stage0.duckdb", read_only=True)
MOBILE = (r"(^|[^a-zа-я0-9])(ios|android|swift|kotlin|flutter|react native|mobile|андроид)"
          r"([^a-zа-я0-9]|$)|мобильн")
ROLE = "Программист, разработчик"

print("=== 30 случайных названий из сегмента ===")
con.sql(f"""
    SELECT name FROM v
    WHERE role = '{ROLE}' AND regexp_matches(lower(name), '{MOBILE}')
    ORDER BY random() LIMIT 30""").show(max_width=200)

print("=== 30 возможных пропусков (в роли, но вне фильтра) ===")
con.sql(f"""
    SELECT name FROM v
    WHERE role = '{ROLE}'
      AND NOT regexp_matches(lower(name), '{MOBILE}')
      AND regexp_matches(lower(name), 'objective|xamarin|kmm|react-native|мобил|прилож|mobile')
    ORDER BY random() LIMIT 30""").show(max_width=200)