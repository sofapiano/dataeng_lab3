"""Загрузка конфигурации пайплайна.

Источники (по убыванию приоритета):
  1. переменные окружения,
  2. файл `configs/pipeline.env` (в репозитории лежит только `pipeline.example.env`),
  3. значения по умолчанию.

Формат файла — `KEY=VALUE`, поддерживаются строки-комментарии `#`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = REPO_ROOT / "configs" / "pipeline.env"

_DEFAULTS = {
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "rustfsadmin",
    "S3_SECRET_KEY": "rustfsadmin",
    "S3_BUCKET": "hh-lake",
    "S3_REGION": "us-east-1",
    "S3_PREFIX_RAW": "raw/",
    "S3_PREFIX_RESULTS": "results/",
    "INPUT_OBJECT": "raw/it_vacancies_2024.csv",
    "LOCAL_INPUT": "data/raw/it_vacancies_2024.csv",
    "OUTPUT_DIR": "data/results",
    "REPORT_DIR": "reports/output",
    "PROFILE_DIR": "profiling/output",
    "CHUNK_SIZE_MB": "64",
    "WORKERS": "4",
    "REDUCERS": "4",
    # На этой машине свободно <1 ГБ RAM, а DuckDB по умолчанию резервирует
    # ~80% физической памяти -> падение на старте с "Memory allocation still
    # failed". Лимиты задаются явно.
    "DUCKDB_MEMORY_LIMIT": "1GB",
    "DUCKDB_THREADS": "2",
}


@dataclass(frozen=True)
class Settings:
    """Значения конфигурации пайплайна."""

    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_region: str
    s3_prefix_raw: str
    s3_prefix_results: str
    input_object: str
    local_input: str
    output_dir: str
    report_dir: str
    profile_dir: str
    chunk_size_mb: int
    workers: int
    reducers: int
    duckdb_memory_limit: str
    duckdb_threads: int
    extra: dict = field(default_factory=dict)

    @property
    def input_uri(self) -> str:
        """Полный S3-URI входного объекта."""
        return f"s3://{self.s3_bucket}/{self.input_object}"

    @property
    def results_prefix(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix_results}"

    @property
    def console_url(self) -> str:
        """URL веб-консоли RustFS (в 1.0.1 она живёт под префиксом /rustfs/console/)."""
        return f"{self.s3_endpoint.rsplit(':', 1)[0]}:9001/rustfs/console/"


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def load_settings(env_file: str | Path | None = None, **overrides) -> Settings:
    """Собирает Settings из файла + окружения + переопределений."""
    path = Path(env_file) if env_file else DEFAULT_ENV_FILE
    file_values = _read_env_file(path)

    merged = dict(_DEFAULTS)
    merged.update(file_values)
    for key in list(merged):
        if key in os.environ:
            merged[key] = os.environ[key]
    merged.update({k: str(v) for k, v in overrides.items() if v is not None})

    known = set(_DEFAULTS) - {
        "CHUNK_SIZE_MB",
        "WORKERS",
        "REDUCERS",
        "DUCKDB_MEMORY_LIMIT",
        "DUCKDB_THREADS",
    }
    extra = {k: v for k, v in merged.items() if k not in known}

    return Settings(
        s3_endpoint=merged["S3_ENDPOINT"],
        s3_access_key=merged["S3_ACCESS_KEY"],
        s3_secret_key=merged["S3_SECRET_KEY"],
        s3_bucket=merged["S3_BUCKET"],
        s3_region=merged["S3_REGION"],
        s3_prefix_raw=merged["S3_PREFIX_RAW"],
        s3_prefix_results=merged["S3_PREFIX_RESULTS"],
        input_object=merged["INPUT_OBJECT"],
        local_input=merged["LOCAL_INPUT"],
        output_dir=merged["OUTPUT_DIR"],
        report_dir=merged["REPORT_DIR"],
        profile_dir=merged["PROFILE_DIR"],
        chunk_size_mb=int(merged["CHUNK_SIZE_MB"]),
        workers=int(merged["WORKERS"]),
        reducers=int(merged["REDUCERS"]),
        duckdb_memory_limit=merged["DUCKDB_MEMORY_LIMIT"],
        duckdb_threads=int(merged["DUCKDB_THREADS"]),
        extra=extra,
    )
