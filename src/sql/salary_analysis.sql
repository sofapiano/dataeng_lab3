-- Этап 5. Влияние навыков на зарплату: пары и тройки навыков.
--
-- Данные читаются напрямую из объектного хранилища (s3://...), тот же CSV,
-- что использовался на этапах 3-4, и то же правило отбора варианта
-- (генерируется из src/common/variant.py).
--
-- Перед этим файлом раннер подставляет src/sql/vacancy_model.sql — общую
-- часть с этапом 6: таблицы vacancy (вакансии варианта и модель зарплаты),
-- vacancy_skill (нормализованные навыки) и skill_pop (популярность навыка
-- во всём варианте). Нумерация разделов продолжает фрагмент.
--
-- Плейсхолдеры {{...}}:
--   {{MIN_PAIR_N}}, {{MIN_TRIPLE_N}}, {{MIN_BASE_N}} — пороги выборки;
--   {{TOP}} — размер итоговых таблиц;
--   {{OUT_*}} — пути локальных CSV для COPY.
--
-- Логика (разделы 1-3 — во фрагменте):
--   4. skill_salary   — медиана зарплаты по навыку в рабочей выборке;
--   5. pair_salary / triple_salary — медиана зарплаты по комбинации;
--   6. *_effects      — прибавка к медиане базового (более частого) навыка.

-- ---------------------------------------------------------------------------
-- 4. Статистики зарплаты по навыку (рабочая выборка: есть валидная зарплата)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE skill_salary AS
SELECT vs.skill,
       count(*) AS n_sal,
       median(v.salary) AS med_sal,
       quantile_cont(v.salary, 0.25) AS p25_sal,
       quantile_cont(v.salary, 0.75) AS p75_sal,
       count(*) FILTER (WHERE v.is_junior) * 1.0
           / nullif(count(v.experience), 0) AS junior_share
FROM vacancy_skill vs
JOIN vacancy v ON v.vacancy_id = vs.vacancy_id
WHERE v.salary_valid
GROUP BY vs.skill;

-- Медиана по рабочей выборке — база для сравнения отдельных навыков.
CREATE OR REPLACE TEMP TABLE salary_baseline AS
SELECT median(salary) AS med_all,
       count(*) AS n_all,
       quantile_cont(salary, 0.25) AS p25,
       quantile_cont(salary, 0.75) AS p75,
       min(salary) AS mn,
       max(salary) AS mx
FROM vacancy
WHERE salary_valid;

-- Состав рабочей выборки: сколько вакансий junior, gross/net, с одной границей.
CREATE OR REPLACE TEMP TABLE sample_stats AS
SELECT count(*) AS n,
       count(*) FILTER (WHERE is_junior) * 1.0 / nullif(count(experience), 0) AS junior_share,
       count(*) FILTER (WHERE lower(gross) = 'true') * 1.0 / count(*) AS gross_share,
       count(*) FILTER (WHERE bounds_count = 1) AS one_bound,
       count(*) FILTER (WHERE bounds_count = 2) AS two_bounds
FROM vacancy
WHERE salary_valid;

-- ---------------------------------------------------------------------------
-- 5. Медиана зарплаты по парам и тройкам навыков
-- ---------------------------------------------------------------------------
-- Вакансия с парой (A, B) попадает в выборку ровно один раз, поэтому count(*)
-- — это число вакансий, а median() — медиана их зарплат.
CREATE OR REPLACE TEMP TABLE pair_salary AS
SELECT a.skill AS skill1,
       b.skill AS skill2,
       count(*) AS n,
       median(v.salary) AS med,
       count(*) FILTER (WHERE v.is_junior) * 1.0
           / nullif(count(v.experience), 0) AS junior_share
FROM vacancy_skill a
JOIN vacancy_skill b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
JOIN vacancy v ON v.vacancy_id = a.vacancy_id
WHERE v.salary_valid
GROUP BY a.skill, b.skill;

CREATE OR REPLACE TEMP TABLE triple_salary AS
SELECT a.skill AS skill1,
       b.skill AS skill2,
       c.skill AS skill3,
       count(*) AS n,
       median(v.salary) AS med,
       count(*) FILTER (WHERE v.is_junior) * 1.0
           / nullif(count(v.experience), 0) AS junior_share
FROM vacancy_skill a
JOIN vacancy_skill b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
JOIN vacancy_skill c ON c.vacancy_id = a.vacancy_id AND b.skill < c.skill
JOIN vacancy v ON v.vacancy_id = a.vacancy_id
WHERE v.salary_valid
GROUP BY a.skill, b.skill, c.skill;

-- ---------------------------------------------------------------------------
-- 6. Прибавка к медиане базового навыка
-- ---------------------------------------------------------------------------
-- Базовый навык комбинации — самый популярный её участник (n_all во всём
-- варианте). Прибавка: медиана(комбинация) - медиана(базового навыка).
-- members разворачивает комбинацию в её участников, QUALIFY оставляет
-- самого популярного из них.
CREATE OR REPLACE TEMP TABLE pair_effects AS
WITH members AS (
    SELECT p.skill1, p.skill2, p.n, p.med AS med_combo, p.junior_share AS junior_combo,
           m.member, pop.n_all, sal.n_sal, sal.med_sal, sal.junior_share AS junior_base
    FROM pair_salary p,
         unnest([p.skill1, p.skill2]) AS m(member)
    JOIN skill_pop pop ON pop.skill = m.member
    JOIN skill_salary sal ON sal.skill = m.member
), based AS (
    SELECT * FROM members
    QUALIFY row_number() OVER (
        PARTITION BY skill1, skill2 ORDER BY n_all DESC, member
    ) = 1
)
SELECT skill1,
       skill2,
       n,
       med_combo,
       member AS base_skill,
       n_sal AS n_base,
       med_sal AS med_base,
       med_combo - med_sal AS delta,
       100.0 * (med_combo - med_sal) / med_sal AS delta_pct,
       junior_combo,
       junior_base
FROM based
WHERE n >= {{MIN_PAIR_N}} AND n_sal >= {{MIN_BASE_N}};

CREATE OR REPLACE TEMP TABLE triple_effects AS
WITH members AS (
    SELECT t.skill1, t.skill2, t.skill3, t.n, t.med AS med_combo,
           t.junior_share AS junior_combo,
           m.member, pop.n_all, sal.n_sal, sal.med_sal, sal.junior_share AS junior_base
    FROM triple_salary t,
         unnest([t.skill1, t.skill2, t.skill3]) AS m(member)
    JOIN skill_pop pop ON pop.skill = m.member
    JOIN skill_salary sal ON sal.skill = m.member
), based AS (
    SELECT * FROM members
    QUALIFY row_number() OVER (
        PARTITION BY skill1, skill2, skill3 ORDER BY n_all DESC, member
    ) = 1
)
SELECT skill1,
       skill2,
       skill3,
       n,
       med_combo,
       member AS base_skill,
       n_sal AS n_base,
       med_sal AS med_base,
       med_combo - med_sal AS delta,
       100.0 * (med_combo - med_sal) / med_sal AS delta_pct,
       junior_combo,
       junior_base
FROM based
WHERE n >= {{MIN_TRIPLE_N}} AND n_sal >= {{MIN_BASE_N}};

-- ---------------------------------------------------------------------------
-- 7. Отдельные навыки: медиана и отклонение от медианы рабочей выборки
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE skill_effects AS
SELECT s.skill,
       p.n_all,
       s.n_sal,
       s.med_sal,
       b.med_all,
       s.med_sal - b.med_all AS delta,
       100.0 * (s.med_sal - b.med_all) / b.med_all AS delta_pct,
       s.p25_sal,
       s.p75_sal,
       s.junior_share
FROM skill_salary s
JOIN skill_pop p ON p.skill = s.skill
CROSS JOIN salary_baseline b
WHERE s.n_sal >= {{MIN_BASE_N}};

-- ---------------------------------------------------------------------------
-- 8. Воронка данных (контрольные числа для отчёта)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE summary AS
SELECT 1 AS step_order, 'вакансий варианта' AS step, count(*) AS n FROM vacancy
UNION ALL SELECT 2, 'с зарплатой (любая валюта)', count(*) FROM vacancy WHERE has_salary
UNION ALL SELECT 3, 'из них в RUR', count(*) FROM vacancy WHERE has_salary AND currency = 'RUR'
UNION ALL SELECT 4, 'RUR и зарплата в допустимом диапазоне', count(*) FROM vacancy WHERE salary_valid
UNION ALL SELECT 5, 'рабочая выборка с навыками',
                  count(DISTINCT vs.vacancy_id)
                  FROM vacancy_skill vs JOIN vacancy v ON v.vacancy_id = vs.vacancy_id
                  WHERE v.salary_valid
UNION ALL SELECT 6, 'уникальных навыков в варианте', count(*) FROM skill_pop
UNION ALL SELECT 7, 'навыков в рабочей выборке', count(*) FROM skill_salary
UNION ALL SELECT 8, 'пар (все)', count(*) FROM pair_salary
UNION ALL SELECT 9, 'пар (после порогов)', count(*) FROM pair_effects
UNION ALL SELECT 10, 'троек (все)', count(*) FROM triple_salary
UNION ALL SELECT 11, 'троек (после порогов)', count(*) FROM triple_effects;

-- ---------------------------------------------------------------------------
-- 9. Выгрузка результатов
-- ---------------------------------------------------------------------------
COPY (SELECT step_order, step, n FROM summary ORDER BY step_order)
TO '{{OUT_SUMMARY}}' (HEADER, DELIMITER ',');

COPY (SELECT skill, n_all, n_sal,
             round(med_sal, 0) AS med_sal,
             round(med_all, 0) AS med_all,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(p25_sal, 0) AS p25_sal,
             round(p75_sal, 0) AS p75_sal,
             round(junior_share, 3) AS junior_share
      FROM skill_effects ORDER BY delta DESC, skill)
TO '{{OUT_SKILLS}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, n,
             round(med_combo, 0) AS med_combo,
             base_skill, n_base,
             round(med_base, 0) AS med_base,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(junior_combo, 3) AS junior_combo,
             round(junior_base, 3) AS junior_base
      FROM pair_effects ORDER BY delta DESC, skill1, skill2)
TO '{{OUT_PAIRS}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, skill3, n,
             round(med_combo, 0) AS med_combo,
             base_skill, n_base,
             round(med_base, 0) AS med_base,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(junior_combo, 3) AS junior_combo,
             round(junior_base, 3) AS junior_base
      FROM triple_effects ORDER BY delta DESC, skill1, skill2, skill3)
TO '{{OUT_TRIPLES}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, n,
             round(med_combo, 0) AS med_combo,
             base_skill, n_base,
             round(med_base, 0) AS med_base,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(junior_combo, 3) AS junior_combo,
             round(junior_base, 3) AS junior_base
      FROM pair_effects ORDER BY delta DESC, skill1, skill2 LIMIT {{TOP}})
TO '{{OUT_PAIRS_UP}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, n,
             round(med_combo, 0) AS med_combo,
             base_skill, n_base,
             round(med_base, 0) AS med_base,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(junior_combo, 3) AS junior_combo,
             round(junior_base, 3) AS junior_base
      FROM pair_effects ORDER BY delta ASC, skill1, skill2 LIMIT {{TOP}})
TO '{{OUT_PAIRS_DOWN}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, skill3, n,
             round(med_combo, 0) AS med_combo,
             base_skill, n_base,
             round(med_base, 0) AS med_base,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(junior_combo, 3) AS junior_combo,
             round(junior_base, 3) AS junior_base
      FROM triple_effects ORDER BY delta DESC, skill1, skill2, skill3 LIMIT {{TOP}})
TO '{{OUT_TRIPLES_UP}}' (HEADER, DELIMITER ',');

COPY (SELECT skill1, skill2, skill3, n,
             round(med_combo, 0) AS med_combo,
             base_skill, n_base,
             round(med_base, 0) AS med_base,
             round(delta, 0) AS delta,
             round(delta_pct, 2) AS delta_pct,
             round(junior_combo, 3) AS junior_combo,
             round(junior_base, 3) AS junior_base
      FROM triple_effects ORDER BY delta ASC, skill1, skill2, skill3 LIMIT {{TOP}})
TO '{{OUT_TRIPLES_DOWN}}' (HEADER, DELIMITER ',');
