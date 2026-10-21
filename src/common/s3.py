"""S3-клиент для RustFS.

RustFS в конфигурации без `RUSTFS_SERVER_DOMAINS` принимает только
path-style адресацию (`http://localhost:9000/hh-lake/key`), поэтому
стиль адресации задан явно.
"""

from __future__ import annotations

from functools import lru_cache

import boto3
from botocore.config import Config

from .config import Settings, load_settings


def make_s3_client(settings: Settings | None = None, **overrides):
    """Создаёт boto3-клиент S3, настроенный на локальный RustFS."""
    settings = settings or load_settings(**overrides)
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=10,
            read_timeout=300,
        ),
    )


@lru_cache(maxsize=1)
def default_client():
    return make_s3_client()
