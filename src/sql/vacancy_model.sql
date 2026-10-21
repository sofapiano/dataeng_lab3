-- Общая часть SQL этапов 5 и 6: вакансии варианта, зарплата и навыки.
--
-- Эти таблицы одинаковы для обоих этапов, поэтому вынесены в отдельный
-- фрагмент, который раннеры подставляют перед своим SQL:
--   1. vacancy       — вакансии варианта с нормализованной зарплатой и флагами;
--   2. vacancy_skill — нормализованные пары (vacancy_id, skill);
--   3. skill_pop     — популярность навыка во всём варианте.
--
-- Плейсхолдеры {{...}}:
--   {{INPUT_URI}}         — s3-URI исходного CSV;
--   {{VARIANT_PREDICATE}} — условие варианта;
--   {{GROSS_TO_NET}}      — коэффициент перевода gross -> net (1 - НДФЛ 13 %);
--   {{SALARY_MIN}}, {{SALARY_MAX}} — границы отсечения выбросов, руб./мес.

-- ---------------------------------------------------------------------------
-- 1. Вакансии варианта: зарплата в рублях и флаги выборки
-- ---------------------------------------------------------------------------
-- Модель зарплаты:
--   * валюта RUR (USD/EUR/UZS — зарубежные вакансии, другой масштаб);
--   * gross приводится к net: зарплата * (1 - 0.13);
--   * если заданы обе границы вилки — берётся середина, если одна — она же;
--   * отбрасываются выбросы вне [{{SALARY_MIN}}, {{SALARY_MAX}}] (руб./мес).
CREATE OR REPLACE TEMP TABLE vacancy AS
WITH raw AS (
    SELECT vacancy_id,
           area,
           experience,
           key_skills,
           currency,
           gross,
           TRY_CAST(salary_from AS DOUBLE) AS salary_from,
           TRY_CAST(salary_to AS DOUBLE) AS salary_to
    FROM read_csv(
            '{{INPUT_URI}}',
            header = true,
            all_varchar = true,
            quote = '"',
            escape = '"',
            strict_mode = false
         ) AS v
    WHERE {{VARIANT_PREDICATE}}
), brut AS (
    SELECT vacancy_id, area, experience, key_skills, currency, gross,
           salary_from IS NOT NULL AS has_from,
           salary_to IS NOT NULL AS has_to,
           CASE
               WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                    AND salary_to >= salary_from
                  THEN (salary_from + salary_to) / 2.0
               WHEN salary_from IS NOT NULL THEN salary_from
               WHEN salary_to IS NOT NULL THEN salary_to
           END AS salary_brut
    FROM raw
)
SELECT vacancy_id,
       area,
       experience,
       key_skills,
       currency,
       gross,
       salary_brut,
       salary_brut * CASE WHEN lower(gross) = 'true' THEN {{GROSS_TO_NET}} ELSE 1.0 END AS salary,
       salary_brut IS NOT NULL AS has_salary,
       coalesce(salary_brut BETWEEN {{SALARY_MIN}} AND {{SALARY_MAX}}, FALSE) AS salary_valid,
       -- сколько границ вилки указано: 2 — «от и до», 1 — только одна
       (has_from::INT + has_to::INT) AS bounds_count,
       -- опыт до 3 лет включительно: 'Нет опыта' и 'От 1 года до 3 лет'
       (experience IN ('Нет опыта', 'От 1 года до 3 лет')) AS is_junior
FROM brut;

-- ---------------------------------------------------------------------------
-- 2. Нормализованные навыки вакансий
-- ---------------------------------------------------------------------------
-- Нормализация совпадает с common.variant.normalize_skills и с этапом 3:
-- split(';'), trim, lower, DISTINCT.
CREATE OR REPLACE TEMP TABLE vacancy_skill AS
SELECT DISTINCT vacancy_id, skill
FROM (
    SELECT v.vacancy_id,
           lower(trim(s.part, ' ' || chr(9) || chr(13) || chr(10))) AS skill
    FROM vacancy v,
         unnest(string_split(coalesce(v.key_skills, ''), ';')) AS s(part)
)
WHERE skill <> '';

-- ---------------------------------------------------------------------------
-- 3. Популярность навыка во всём варианте
-- ---------------------------------------------------------------------------
-- По n_all выбирается базовый навык в этапе 5 и считается относительная
-- востребованность навыка в регионе в этапе 6.
CREATE OR REPLACE TEMP TABLE skill_pop AS
SELECT skill, count(*) AS n_all
FROM vacancy_skill
GROUP BY skill;
