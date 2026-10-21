"""Единое описание варианта задания и правила отбора вакансий.

Ключевая идея: правило отбора и правила нормализации навыков заданы ОДИН РАЗ
здесь, а MapReduce (Python) и DuckDB (SQL) используют именно эти константы.
Иначе расхождение чисел на этапе 4 неизбежно.

Вариант 9 — «Мобильная разработка».

Правило отбора (одно и то же в обоих движках):
    role = 'Программист, разработчик'
    И  lower(name) соответствует регулярному выражению
        (^|[^a-zа-я0-9])(ios|android|swift|kotlin|flutter|react native|mobile|андроид)([^a-zа-я0-9]|$)|мобильн

Регулярное выражение записано в синтаксисе, одинаковом для Python `re`
и DuckDB `regexp_matches` (RE2): класс символов с кириллицей, группа
с альтернативами, якоря границ слова.
"""

from __future__ import annotations

import re

# --- Вариант ---------------------------------------------------------------

VARIANT_NUMBER = 9
VARIANT_TITLE = "Мобильная разработка"

#: Роль вакансии, в которой ищем мобильную разработку.
VARIANT_ROLE = "Программист, разработчик"

#: Ключевые слова мобильной разработки в названии вакансии.
VARIANT_NAME_REGEX = (
    r"(^|[^a-zа-я0-9])(ios|android|swift|kotlin|flutter|react native|mobile|андроид)"
    r"([^a-zа-я0-9]|$)|мобильн"
)

VARIANT_DESCRIPTION = (
    "Вариант 9 — мобильная разработка. Вакансия относится к сегменту, если её "
    "роль равна «Программист, разработчик», а в названии (в нижнем регистре) "
    "встречается хотя бы один мобильный маркер: ios, android, swift, kotlin, "
    "flutter, react native, mobile, андроид, либо слово «мобильн*»."
)

#: Сводное описание варианта — используется в отчёте и заголовках результатов.
VARIANT = {
    "number": VARIANT_NUMBER,
    "title": VARIANT_TITLE,
    "role": VARIANT_ROLE,
    "name_regex": VARIANT_NAME_REGEX,
    "description": VARIANT_DESCRIPTION,
}

_NAME_RE = re.compile(VARIANT_NAME_REGEX)


def match_variant(name: str | None, role: str | None) -> bool:
    """Проверяет, относится ли вакансия к варианту. Аналог SQL-предиката."""
    if role != VARIANT_ROLE:
        return False
    if not name:
        return False
    return _NAME_RE.search(name.lower()) is not None


def filter_predicate_sql(alias: str = "v") -> str:
    """SQL-предикат отбора вариантом. Использует ту же константу регулярки.

    Возвращает готовый фрагмент для WHERE, например:
        v.role = 'Программист, разработчик'
        AND regexp_matches(lower(v.name), '<тот же regex>')
    """
    pattern = VARIANT_NAME_REGEX.replace("'", "''")
    role = VARIANT_ROLE.replace("'", "''")
    return (
        f"{alias}.role = '{role}' "
        f"AND regexp_matches(lower({alias}.name), '{pattern}')"
    )


# --- Навыки ----------------------------------------------------------------

#: Разделитель навыков в колонке key_skills.
SKILL_SEPARATOR = ";"

#: Навыки, которые не несут смысловой нагрузки (служебные/шумные) и
#: исключаются из подсчёта. Пустой список — навыки считаются как есть.
SKILL_STOPLIST: frozenset[str] = frozenset()


def normalize_skills(raw: str | None) -> list[str]:
    """Нормализует поле key_skills в список уникальных навыков.

    Правила:
      * строка режется по ';',
      * каждый навык обрезается по пробелам и приводится к нижнему регистру,
      * пустые навыки отбрасываются,
      * дубли внутри одной вакансии убираются (сохраняется порядок первого
        появления), результат сортируется — чтобы комбинации (A,B) и (B,A)
        давали один и тот же ключ.
    """
    if not raw:
        return []
    seen: dict[str, None] = {}
    for part in raw.split(SKILL_SEPARATOR):
        skill = part.strip().lower()
        if not skill or skill in SKILL_STOPLIST:
            continue
        seen.setdefault(skill, None)
    return sorted(seen)
