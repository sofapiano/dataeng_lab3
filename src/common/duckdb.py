"""Общие помощники для DuckDB: подключение к S3 и подстановка параметров в SQL.

Вынесены в отдельный модуль, потому что к DuckDB подключаются два скрипта
(этап 3б и этап 5) и настройки httpfs/S3 у них должны совпадать.
"""

from __future__ import annotations

import os
import time

# DuckDB тянет OpenBLAS, который на старте резервирует буфер под каждый поток.
# Из-за ограничений памяти переменная должна быть выставлена ДО
# импорта duckdb, иначе импорт падает с "Memory allocation still failed".
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

import duckdb  # noqa: E402

from .config import Settings  # noqa: E402


def connect(
    settings: Settings,
    memory_limit: str | None = None,
    threads: int | None = None,
) -> duckdb.DuckDBPyConnection:
    """Подключение к DuckDB с лимитами памяти и доступом к RustFS через httpfs.

    memory_limit, threads — переопределения из конфигурации (например, когда
    нужно уложиться в свободную память машины при чтении большого CSV).
    """
    con = duckdb.connect(
        config={
            "memory_limit": memory_limit or settings.duckdb_memory_limit,
            "threads": threads or settings.duckdb_threads,
            "preserve_insertion_order": False,
        }
    )
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{settings.s3_endpoint.replace("http://", "")}';
        SET s3_access_key_id='{settings.s3_access_key}';
        SET s3_secret_access_key='{settings.s3_secret_key}';
        SET s3_region='{settings.s3_region}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)
    return con


def render(template: str, values: dict[str, str]) -> str:
    """Подставляет шаблон вида {{NAME}} значениями из словаря.

    Незаполненные плейсхолдеры — ошибка: молча получить SQL с '{{...}}'
    внутри и упасть где-то в DuckDB неудобно.
    """
    sql = template
    for name, value in values.items():
        sql = sql.replace("{{" + name + "}}", value)
    missing = [
        line for line in sql.splitlines()
        if "{{" in line and not line.strip().startswith("--")
    ]
    if missing:
        raise ValueError(f"не подставлены плейсхолдеры: {missing}")
    return sql


def run_statements(con: duckdb.DuckDBPyConnection, sql: str, on_statement=None) -> None:
    """Выполняет скрипт по инструкциям, сообщая о каждой через on_statement.

    on_statement(номер, секунды, первая содержательная строка инструкции) —
    обычно это печать строки в транскрипт отчёта.
    """
    for i, statement in enumerate(con.extract_statements(sql), 1):
        query = statement.query
        first_line = next(
            (ln.strip() for ln in query.splitlines()
             if ln.strip() and not ln.strip().startswith("--")),
            "",
        )
        started = time.perf_counter()
        con.execute(query)
        if on_statement is not None:
            on_statement(i, time.perf_counter() - started, first_line)
