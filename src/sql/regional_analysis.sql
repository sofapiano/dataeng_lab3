-- Этап 6. Региональный анализ: три крупнейших региона варианта.
--
-- Перед этим файлом раннер подставляет src/sql/vacancy_model.sql — общую
-- часть с этапом 5 (vacancy, vacancy_skill, skill_pop), поэтому модель
-- зарплаты и нормализация навыков здесь те же. Нумерация разделов продолжает
-- фрагмент.
--
-- Плейсхолдеры {{...}}:
--   {{REGION_LIMIT}}       — сколько регионов сравниваем;
--   {{MIN_REGION_SKILL_N}} — минимальная частота навыка в регионе для индекса
--                            относительной востребованности (иначе шум);
--   {{MIN_SKILL_TOTAL_N}}  — минимальная частота навыка во всём варианте:
--                            редкий навык одного региона даёт индекс ×20
--                            на пяти вакансиях, это не специализация;
--   {{TOP}}                — размер выгрузок «топ на регион»;
--   {{OUT_*}}              — пути локальных CSV для COPY.
--
-- Логика:
--   4. area_counts     — все регионы варианта (контекст: сколько их и какие);
--   5. region          — регионы сравнения: самые крупные по числу вакансий;
--   6. region_base     — база для долей: вакансии и вакансии с навыками;
--   7. region_salary   — зарплатные показатели региона;
--   8. region_skill    — востребованность навыка в регионе и индекс к варианту;
--   9. region_pair / region_triple — комбинации навыков по регионам;
--  10. region_experience — медиана по региону и опыту (проверка состава).

-- ---------------------------------------------------------------------------
-- 4. Все регионы варианта
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE area_counts AS
SELECT area, count(*) AS n_vac
FROM vacancy
WHERE area IS NOT NULL AND area <> ''
GROUP BY area;

-- ---------------------------------------------------------------------------
-- 5. Регионы сравнения: самые крупные по числу вакансий варианта
-- ---------------------------------------------------------------------------
-- Правило выбора одно и то же при любом запуске: берём top-N по числу
-- вакансий, чтобы в каждом регионе хватило данных на медианы и навыки.
CREATE OR REPLACE TEMP TABLE region AS
SELECT area, n_vac, row_number() OVER (ORDER BY n_vac DESC, area) AS region_rank
FROM area_counts
ORDER BY region_rank
LIMIT {{REGION_LIMIT}};

-- ---------------------------------------------------------------------------
-- 6. База для долей: вакансии и вакансии с навыками
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE region_base AS
SELECT r.region_rank,
       r.area,
       count(DISTINCT v.vacancy_id) AS n_vac,
       count(DISTINCT vs.vacancy_id) AS n_with_skills
FROM vacancy v
JOIN region r ON r.area = v.area
LEFT JOIN vacancy_skill vs ON vs.vacancy_id = v.vacancy_id
GROUP BY r.region_rank, r.area;

-- Вакансии с навыками по всему варианту — база для share_variant.
CREATE OR REPLACE TEMP TABLE variant_base AS
SELECT count(DISTINCT vacancy_id) AS n_with_skills
FROM vacancy_skill;

-- ---------------------------------------------------------------------------
-- 7. Зарплатные показатели региона
-- ---------------------------------------------------------------------------
-- salary_share — доля вакансий региона, где зарплата вообще указана (любая
-- валюта). Остальные метрики — только по валидной зарплате (RUR, не выброс).
CREATE OR REPLACE TEMP TABLE region_salary AS
SELECT r.region_rank,
       r.area,
       b.n_vac,
       b.n_with_skills,
       count(*) FILTER (WHERE v.has_salary) AS n_any_currency,
       count(*) FILTER (WHERE v.salary_valid) AS n_valid,
       count(*) FILTER (WHERE v.salary_valid) * 1.0 / b.n_vac AS salary_share,
       median(v.salary) FILTER (WHERE v.salary_valid) AS med,
       quantile_cont(v.salary, 0.25) FILTER (WHERE v.salary_valid) AS p25,
       quantile_cont(v.salary, 0.75) FILTER (WHERE v.salary_valid) AS p75,
       count(*) FILTER (WHERE v.salary_valid AND v.is_junior) * 1.0
           / nullif(count(*) FILTER (WHERE v.salary_valid), 0) AS junior_share,
       count(*) FILTER (WHERE v.salary_valid AND lower(v.gross) = 'true') * 1.0
           / nullif(count(*) FILTER (WHERE v.salary_valid), 0) AS gross_share
FROM vacancy v
JOIN region r ON r.area = v.area
JOIN region_base b ON b.area = r.area
GROUP BY r.region_rank, r.area, b.n_vac, b.n_with_skills;

-- ---------------------------------------------------------------------------
-- 8. Востребованность навыка в регионе
-- ---------------------------------------------------------------------------
-- share_region  — доля вакансий региона с навыком;
-- share_variant — доля вакансий всего варианта с навыком;
-- demand_index  — share_region / share_variant: > 1 — навык в регионе
--                 встречается чаще, чем в среднем по варианту.
CREATE OR REPLACE TEMP TABLE region_skill AS
SELECT r.region_rank,
       r.area,
       vs.skill,
       count(*) AS n,
       sp.n_all,
       count(*) * 1.0 / b.n_with_skills AS share_region,
       sp.n_all * 1.0 / vb.n_with_skills AS share_variant,
       (count(*) * 1.0 / b.n_with_skills)
           / (sp.n_all * 1.0 / vb.n_with_skills) AS demand_index
FROM vacancy v
JOIN region r ON r.area = v.area
JOIN vacancy_skill vs ON vs.vacancy_id = v.vacancy_id
JOIN region_base b ON b.area = r.area
JOIN skill_pop sp ON sp.skill = vs.skill
CROSS JOIN variant_base vb
GROUP BY r.region_rank, r.area, vs.skill, b.n_with_skills, sp.n_all, vb.n_with_skills;

-- ---------------------------------------------------------------------------
-- 9. Пары и тройки навыков по регионам (частота)
-- ---------------------------------------------------------------------------
-- Вакансия с парой (A, B) попадает в выборку ровно один раз, поэтому count(*)
-- — число вакансий региона, где встречаются оба навыка.
CREATE OR REPLACE TEMP TABLE region_pair AS
SELECT r.region_rank,
       r.area,
       a.skill AS skill1,
       b.skill AS skill2,
       count(*) AS n
FROM vacancy v
JOIN region r ON r.area = v.area
JOIN vacancy_skill a ON a.vacancy_id = v.vacancy_id
JOIN vacancy_skill b ON b.vacancy_id = v.vacancy_id AND a.skill < b.skill
GROUP BY r.region_rank, r.area, a.skill, b.skill;

CREATE OR REPLACE TEMP TABLE region_triple AS
SELECT r.region_rank,
       r.area,
       a.skill AS skill1,
       b.skill AS skill2,
       c.skill AS skill3,
       count(*) AS n
FROM vacancy v
JOIN region r ON r.area = v.area
JOIN vacancy_skill a ON a.vacancy_id = v.vacancy_id
JOIN vacancy_skill b ON b.vacancy_id = v.vacancy_id AND a.skill < b.skill
JOIN vacancy_skill c ON c.vacancy_id = v.vacancy_id AND b.skill < c.skill
GROUP BY r.region_rank, r.area, a.skill, b.skill, c.skill;

-- ---------------------------------------------------------------------------
-- 10. Медиана зарплаты по региону и опыту
-- ---------------------------------------------------------------------------
-- Если разрыв между регионами сохраняется внутри одного опыта, то он не
-- объясняется составом вакансий по грейдам.
CREATE OR REPLACE TEMP TABLE region_experience AS
SELECT r.region_rank,
       r.area,
       coalesce(v.experience, '—') AS experience,
       count(*) AS n,
       median(v.salary) AS med
FROM vacancy v
JOIN region r ON r.area = v.area
WHERE v.salary_valid
GROUP BY r.region_rank, r.area, coalesce(v.experience, '—');

-- ---------------------------------------------------------------------------
-- 11. Выгрузка результатов
-- ---------------------------------------------------------------------------
COPY (SELECT region_rank,
             area,
             n_vac,
             n_with_skills,
             n_any_currency,
             n_valid,
             round(salary_share, 3) AS salary_share,
             CAST(round(med) AS BIGINT) AS med,
             CAST(round(p25) AS BIGINT) AS p25,
             CAST(round(p75) AS BIGINT) AS p75,
             round(junior_share, 3) AS junior_share,
             round(gross_share, 3) AS gross_share
      FROM region_salary ORDER BY region_rank)
TO '{{OUT_REGIONS}}' (HEADER, DELIMITER ',');

COPY (SELECT region_rank, area, skill, n,
             round(share_region, 4) AS share_region,
             round(share_variant, 4) AS share_variant,
             round(demand_index, 2) AS demand_index
      FROM region_skill
      QUALIFY row_number() OVER (PARTITION BY area ORDER BY n DESC, skill) <= {{TOP}}
      ORDER BY region_rank, n DESC, skill)
TO '{{OUT_SKILLS_TOP}}' (HEADER, DELIMITER ',');

COPY (SELECT region_rank, area, skill, n,
             round(share_region, 4) AS share_region,
             round(share_variant, 4) AS share_variant,
             round(demand_index, 2) AS demand_index
      FROM region_skill
      WHERE n >= {{MIN_REGION_SKILL_N}} AND n_all >= {{MIN_SKILL_TOTAL_N}}
      QUALIFY row_number() OVER (PARTITION BY area ORDER BY demand_index DESC, skill) <= {{TOP}}
      ORDER BY region_rank, demand_index DESC, skill)
TO '{{OUT_SKILLS_INDEX}}' (HEADER, DELIMITER ',');

COPY (SELECT region_rank, area, skill1, skill2, n
      FROM region_pair
      QUALIFY row_number() OVER (PARTITION BY area ORDER BY n DESC, skill1, skill2) <= {{TOP}}
      ORDER BY region_rank, n DESC, skill1, skill2)
TO '{{OUT_PAIRS}}' (HEADER, DELIMITER ',');

COPY (SELECT region_rank, area, skill1, skill2, skill3, n
      FROM region_triple
      QUALIFY row_number() OVER (PARTITION BY area ORDER BY n DESC, skill1, skill2, skill3) <= {{TOP}}
      ORDER BY region_rank, n DESC, skill1, skill2, skill3)
TO '{{OUT_TRIPLES}}' (HEADER, DELIMITER ',');

COPY (SELECT region_rank, area, experience, n, CAST(round(med) AS BIGINT) AS med
      FROM region_experience
      ORDER BY region_rank, n DESC, experience)
TO '{{OUT_EXPERIENCE}}' (HEADER, DELIMITER ',');
