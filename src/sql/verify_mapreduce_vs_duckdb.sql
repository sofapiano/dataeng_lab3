-- Этап 4. Сверка результатов MapReduce и DuckDB.
--
-- Оба набора читаются из объектного хранилища (файлы уже скачаны
-- scripts/verify.py) и сравниваются FULL OUTER JOIN по ключу:
--   * only_mr  — ключ есть только у MapReduce (потерян в DuckDB);
--   * only_db  — ключ есть только у DuckDB (лишний);
--   * diff_cnt — ключ есть в обоих, но частоты разные.
-- Ожидаемый результат: 0 / 0 / 0.
--
-- {{MR_KEY}} / {{DB_KEY}} подставляет scripts/verify.py (имена колонок
-- в файлах различаются: у MapReduce для k=1 колонка называется skill,
-- у DuckDB — skill1):
--   k=1: skill / skill1
--   k=2: concat_ws(chr(31), skill1, skill2) — одинаково с обеих сторон
--   k=3: concat_ws(chr(31), skill1, skill2, skill3) — одинаково

WITH mr AS (
    SELECT {{MR_KEY}} AS key, CAST("count" AS BIGINT) AS mr_cnt
    FROM read_csv('{{MR_PATH}}', header = true, all_varchar = true)
),
db AS (
    SELECT {{DB_KEY}} AS key, CAST("count" AS BIGINT) AS db_cnt
    FROM read_csv('{{DB_PATH}}', header = true, all_varchar = true)
)
SELECT
    count(*) FILTER (WHERE db.key IS NULL)                    AS only_mr,
    count(*) FILTER (WHERE mr.key IS NULL)                    AS only_db,
    count(*) FILTER (WHERE mr.mr_cnt IS DISTINCT FROM db.db_cnt) AS diff_cnt,
    count(*)                                                  AS total_keys,
    sum(coalesce(mr.mr_cnt, 0))                               AS mr_total,
    sum(coalesce(db.db_cnt, 0))                               AS db_total
FROM mr
FULL OUTER JOIN db ON mr.key = db.key;
