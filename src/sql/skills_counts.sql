-- Этап 3б. Частоты навыков, пар и троек одним SQL-запросом к DuckDB.
--
-- Данные читаются напрямую из объектного хранилища (s3://...), а не с диска.
-- Плейсхолдеры {{...}} подставляет scripts/run_duckdb.py:
--   {{INPUT_URI}}         — s3-URI исходного CSV
--   {{VARIANT_PREDICATE}} — условие варианта (генерируется из src/common/variant.py,
--                           то есть ТО ЖЕ правило, что использует MapReduce)
--   {{OUT_K1}} ... {{OUT_SUMMARY}} — пути локальных CSV для COPY
--
-- Логика:
--   1. variant       — только вакансии варианта;
--   2. vacancy_skill — нормализованные пары (vacancy_id, skill): split по ';',
--                      trim, lower, DISTINCT. DISTINCT обязателен: в данных
--                      есть вакансии с дублями навыков (147 штук на этапе 0).
--   3. skills_k1/k2/k3 — частоты: k=1 через GROUP BY, k=2/k=3 через self-join
--                      с условием skill1 < skill2 < skill3 (одна и та же
--                      комбинация не зависит от порядка навыков в вакансии).
--   4. skill_histogram — сколько навыков в вакансии (для проверки комбинаторики).

CREATE OR REPLACE TEMP VIEW variant AS
SELECT v.vacancy_id, v.name, v.key_skills
FROM read_csv(
        '{{INPUT_URI}}',
        header = true,
        all_varchar = true,
        quote = '"',
        escape = '"',
        strict_mode = false
     ) AS v
WHERE {{VARIANT_PREDICATE}};

-- Уникальные навыки вакансии. Нормализация должна совпадать с
-- common.variant.normalize_skills: split(';'), strip(' \t\r\n'), lower, DISTINCT.
CREATE OR REPLACE TEMP TABLE vacancy_skill AS
SELECT DISTINCT vacancy_id, skill
FROM (
    SELECT vacancy_id, lower(trim(s, ' ' || chr(9) || chr(13) || chr(10))) AS skill
    FROM variant, unnest(string_split(coalesce(key_skills, ''), ';')) AS t(s)
)
WHERE skill <> '';

-- k = 1: отдельные навыки.
CREATE OR REPLACE TEMP TABLE skills_k1 AS
SELECT skill AS skill1, count(*) AS cnt
FROM vacancy_skill
GROUP BY skill;

-- k = 2: пары. a.skill < b.skill исключает (B,A) и не даёт посчитать пару дважды.
CREATE OR REPLACE TEMP TABLE skills_k2 AS
SELECT a.skill AS skill1, b.skill AS skill2, count(*) AS cnt
FROM vacancy_skill a
JOIN vacancy_skill b
  ON a.vacancy_id = b.vacancy_id
 AND a.skill < b.skill
GROUP BY a.skill, b.skill;

-- k = 3: тройки, аналогично a < b < c.
CREATE OR REPLACE TEMP TABLE skills_k3 AS
SELECT a.skill AS skill1, b.skill AS skill2, c.skill AS skill3, count(*) AS cnt
FROM vacancy_skill a
JOIN vacancy_skill b
  ON a.vacancy_id = b.vacancy_id
 AND a.skill < b.skill
JOIN vacancy_skill c
  ON c.vacancy_id = a.vacancy_id
 AND b.skill < c.skill
GROUP BY a.skill, b.skill, c.skill;

-- Сколько навыков в вакансии (только вакансии с навыками).
CREATE OR REPLACE TEMP TABLE skill_histogram AS
SELECT n AS skills_per_vacancy, count(*) AS vacancies
FROM (
    SELECT vacancy_id, count(*) AS n
    FROM vacancy_skill
    GROUP BY vacancy_id
)
GROUP BY n;

-- Контрольные числа: число уникальных ключей и сумма частот по каждому k,
-- плюс теоретическое число вхождений C(n,k) по гистограмме.
CREATE OR REPLACE TEMP TABLE summary AS
WITH counts AS (
    SELECT 1 AS k, count(*) AS unique_keys, sum(cnt) AS total FROM skills_k1
    UNION ALL
    SELECT 2, count(*), sum(cnt) FROM skills_k2
    UNION ALL
    SELECT 3, count(*), sum(cnt) FROM skills_k3
), expected AS (
    SELECT k,
           sum(CASE k WHEN 1 THEN n
                      WHEN 2 THEN n * (n - 1) // 2
                      ELSE n * (n - 1) * (n - 2) // 6 END * vacancies) AS expected
    FROM (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) AS ks
    CROSS JOIN (SELECT skills_per_vacancy AS n, vacancies FROM skill_histogram)
    GROUP BY k
)
SELECT c.k,
       c.unique_keys,
       c.total AS total_occurrences,
       e.expected AS expected_from_histogram
FROM counts c
JOIN expected e USING (k)
ORDER BY c.k;

COPY (SELECT skill1, cnt AS count FROM skills_k1 ORDER BY cnt DESC, skill1)
TO '{{OUT_K1}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, cnt AS count FROM skills_k2 ORDER BY cnt DESC, skill1, skill2)
TO '{{OUT_K2}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, skill3, cnt AS count FROM skills_k3 ORDER BY cnt DESC, skill1, skill2, skill3)
TO '{{OUT_K3}}' (HEADER, DELIMITER ',');

COPY (SELECT skills_per_vacancy, vacancies FROM skill_histogram ORDER BY skills_per_vacancy)
TO '{{OUT_HIST}}' (HEADER, DELIMITER ',');

COPY (SELECT * FROM summary)
TO '{{OUT_SUMMARY}}' (HEADER, DELIMITER ',');
