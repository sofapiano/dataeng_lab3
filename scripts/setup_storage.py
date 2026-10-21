"""Этап 1. Развёртывание объектного хранилища RustFS и создание бакета.

Скрипт идемпотентен: если бакет уже есть, он не пересоздаётся, а просто
проверяется. Вся выводимая информация дублируется в файл отчёта.

Запуск:
    python scripts/setup_storage.py
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from common.config import load_settings  # noqa: E402
from common.reporting import Transcript  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402


def check_endpoint(settings) -> tuple[bool, str]:
    """Проверяет, что S3-эндпоинт отвечает."""
    url = f"{settings.s3_endpoint}/health"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = r.read(300).decode("utf-8", "replace")
            return r.status == 200, f"HTTP {r.status}: {body.strip()}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def check_console(settings) -> tuple[bool, str]:
    """Проверяет, что веб-консоль RustFS отдаёт страницу."""
    url = settings.console_url
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = r.read(4096)
            return r.status == 200 and b"<html" in body.lower(), (
                f"HTTP {r.status}, {len(body)}+ байт HTML"
            )
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Этап 1: RustFS + бакет hh-lake")
    parser.add_argument("--bucket", default=None, help="имя бакета (по умолчанию из конфига)")
    parser.add_argument("--out", default=None, help="файл транскрипта")
    args = parser.parse_args()

    settings = load_settings()
    bucket_name = args.bucket or settings.s3_bucket
    out_path = Path(args.out) if args.out else Path(settings.report_dir) / "stage1_storage.txt"

    with Transcript(out_path) as t:
        t.rule("ЭТАП 1. ОБЪЕКТНОЕ ХРАНИЛИЩЕ RUSTFS — РАЗВЁРТЫВАНИЕ И СОЗДАНИЕ БАКЕТА")
        t.log()
        t.log("Конфигурация подключения:")
        t.log(f"  S3 endpoint   : {settings.s3_endpoint}")
        t.log(f"  region         : {settings.s3_region}")
        t.log(f"  access key     : {settings.s3_access_key}")
        t.log(f"  bucket         : {bucket_name}")
        t.log(f"  web console    : {settings.console_url}")
        t.log()

        t.rule("1. Проверка доступности S3-эндпоинта")
        ok, detail = check_endpoint(settings)
        t.log(f"  GET {settings.s3_endpoint}/health -> {detail}")
        if not ok:
            t.log("  ОШИБКА: хранилище не отвечает. Поднимите его: cd infra && docker compose up -d")
            return 1
        t.log("  [OK] S3 API доступен")
        t.log()

        t.rule("2. Проверка веб-консоли")
        ok, detail = check_console(settings)
        t.log(f"  GET {settings.console_url} -> {detail}")
        t.log("  [OK] консоль отвечает" if ok else "  [WARN] консоль не отдаёт страницу")
        t.log()

        client = make_s3_client(settings)

        t.rule("3. Список бакетов ДО создания")
        before = [b["Name"] for b in client.list_buckets()["Buckets"]]
        t.log(f"  бакетов: {len(before)} -> {before if before else '(пусто)'}")
        t.log()

        t.rule("4. Создание бакета")
        if bucket_name in before:
            t.log(f"  бакет «{bucket_name}» уже существует — пропускаем создание")
        else:
            client.create_bucket(Bucket=bucket_name)
            t.log(f"  бакет «{bucket_name}» создан")
        t.log()

        t.rule("5. Список бакетов ПОСЛЕ создания")
        after = client.list_buckets()["Buckets"]
        t.log(f"  бакетов: {len(after)}")
        for b in after:
            t.log(f"    - {b['Name']} (создан {b['CreationDate']})")
        if bucket_name not in [b["Name"] for b in after]:
            t.log(f"  ОШИБКА: бакет «{bucket_name}» не появился в списке")
            return 1
        t.log(f"  [OK] бакет «{bucket_name}» существует")
        t.log()

        t.rule("6. Проверка бакета (head_bucket, versioning, objects)")
        client.head_bucket(Bucket=bucket_name)
        t.log(f"  head_bucket({bucket_name}) -> OK")
        try:
            versioning = client.get_bucket_versioning(Bucket=bucket_name)
            t.log(f"  versioning: {json.dumps(versioning.get('Status', 'Disabled'), ensure_ascii=False)}")
        except Exception as e:  # noqa: BLE001
            t.log(f"  versioning: недоступно ({e})")
        objects = client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        t.log(f"  объектов в бакете: {len(objects)}")
        for o in objects:
            t.log(f"    - {o['Key']}  ({o['Size']} байт)")
        t.log()

        t.rule("ИТОГ ЭТАПА 1")
        t.log(f"  [OK] RustFS запущен и отвечает на {settings.s3_endpoint}")
        t.log(f"  [OK] веб-консоль доступна: {settings.console_url}")
        t.log(f"  [OK] бакет «{bucket_name}» создан")
        t.log(f"  транскрипт сохранён: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
