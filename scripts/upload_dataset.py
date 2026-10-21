"""Этап 2. Загрузка датасета в объектное хранилище RustFS.

Загружает локальный CSV в бакет `hh-lake` (multipart-загрузка, устойчива к
обрывам) и проверяет, что объект в хранилище совпадает с локальным файлом.

Запуск:
    python scripts/upload_dataset.py             # загрузить, если объекта нет
    python scripts/upload_dataset.py --force     # перезагрузить принудительно
    python scripts/upload_dataset.py --verify    # только проверить, без загрузки
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from boto3.s3.transfer import TransferConfig  # noqa: E402

from common.config import load_settings  # noqa: E402
from common.hashing import md5_file  # noqa: E402
from common.reporting import Transcript  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402

#: 32 МБ — размер Multipart-части. RustFS спокойно принимает части до 5 ГБ,
#: но 32 МБ дают разумный баланс между памятью, числом запросов и нагрузкой
#: на диск (для 4-xor erasure-кода каждый запрос пишет на все 4 диска).
PART_SIZE = 32 * 1024 * 1024
MAX_CONCURRENCY = 2
MAX_ATTEMPTS = 3


def abort_stale_multipart(client, bucket: str, key: str) -> int:
    """Прерывает незавершённые multipart-загрузки по ключу.

    После аварийной загрузки брошенные части продолжают занимать место и
    мешают quorum'у; RustFS может отдавать по ним 503 SlowDownRead.
    """
    aborted = 0
    try:
        uploads = client.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
    except Exception:  # noqa: BLE001
        return 0
    for up in uploads:
        if up.get("Key") == key:
            try:
                client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=up["UploadId"]
                )
                aborted += 1
            except Exception:  # noqa: BLE001
                pass
    return aborted


def human(n: float) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "Б" else f"{int(n)} Б"
        n /= 1024
    return f"{n:.1f} ПБ"


def local_md5(path: Path, block: int = 1 << 22) -> str:
    """MD5 локального файла (совпадает с ETag одиночной загрузки S3)."""
    return md5_file(path, block)


def head(client, bucket: str, key: str):
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Этап 2: загрузка датасета в RustFS")
    parser.add_argument("--local", default=None, help="путь к локальному CSV")
    parser.add_argument("--key", default=None, help="ключ объекта в бакете")
    parser.add_argument("--force", action="store_true", help="перезагрузить, даже если объект есть")
    parser.add_argument("--verify", action="store_true", help="только проверить объект, не загружать")
    parser.add_argument("--out", default=None, help="файл транскрипта")
    args = parser.parse_args()

    settings = load_settings()
    local_path = Path(args.local or settings.local_input)
    key = args.key or settings.input_object
    out_path = Path(args.out) if args.out else Path(settings.report_dir) / "stage2_upload.txt"

    if not local_path.is_file():
        print(f"ОШИБКА: локальный файл не найден: {local_path}")
        return 1

    local_size = local_path.stat().st_size
    client = make_s3_client(settings)

    with Transcript(out_path) as t:
        t.rule("ЭТАП 2. ЗАГРУЗКА ДАТАСЕТА В ОБЪЕКТНОЕ ХРАНИЛИЩЕ")
        t.log()
        t.log("Параметры загрузки:")
        t.log(f"  endpoint       : {settings.s3_endpoint}")
        t.log(f"  bucket         : {settings.s3_bucket}")
        t.log(f"  local file     : {local_path}")
        t.log(f"  local size     : {human(local_size)} ({local_size} байт)")
        t.log(f"  target key     : s3://{settings.s3_bucket}/{key}")
        t.log(f"  multipart part : {human(PART_SIZE)}, concurrency: {MAX_CONCURRENCY}")
        t.log()

        existing = head(client, settings.s3_bucket, key)
        t.rule("1. Состояние объекта ДО загрузки")
        if existing:
            t.log(f"  объект существует: {human(existing['ContentLength'])} "
                  f"({existing['ContentLength']} байт), ETag={existing['ETag']}")
        else:
            t.log("  объекта в бакете нет")
        t.log()

        if args.verify:
            t.rule("2. Только проверка")
        elif existing and not args.force:
            t.rule("2. Загрузка пропущена")
            t.log("  объект уже загружен и совпадает по размеру; используйте --force для перезагрузки")
        else:
            t.rule("2. Загрузка (multipart)")
            cfg = TransferConfig(
                multipart_threshold=PART_SIZE,
                multipart_chunksize=PART_SIZE,
                max_concurrency=MAX_CONCURRENCY,
                use_threads=True,
            )
            started = time.perf_counter()
            last_error: Exception | None = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    client.upload_file(
                        str(local_path),
                        settings.s3_bucket,
                        key,
                        Config=cfg,
                        ExtraArgs={"ContentType": "text/csv"},
                    )
                    last_error = None
                    break
                except Exception as e:  # noqa: BLE001
                    last_error = e
                    t.log(f"  попытка {attempt}/{MAX_ATTEMPTS} не удалась: "
                          f"{type(e).__name__}: {str(e)[:160]}")
                    # Убираем брошенные части, иначе следующая попытка стартует
                    # на забитом пуле и получит 503 SlowDownRead.
                    n = abort_stale_multipart(client, settings.s3_bucket, key)
                    t.log(f"  прервано незавершённых multipart-загрузок: {n}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(5 * attempt)
            if last_error is not None:
                t.log(f"  ОШИБКА загрузки: {type(last_error).__name__}: {last_error}")
                return 1
            elapsed = time.perf_counter() - started
            t.log(f"  загружено за {elapsed:.1f} с "
                  f"({human(local_size / max(elapsed, 1e-9))}/с)")
        t.log()

        t.rule("3. Верификация объекта в хранилище")
        obj = head(client, settings.s3_bucket, key)
        if not obj:
            t.log("  ОШИБКА: объект не найден после загрузки")
            return 1
        remote_size = obj["ContentLength"]
        t.log(f"  s3://{settings.s3_bucket}/{key}")
        t.log(f"    размер в хранилище : {human(remote_size)} ({remote_size} байт)")
        t.log(f"    размер локально     : {human(local_size)} ({local_size} байт)")
        t.log(f"    ETag                : {obj['ETag']}")
        t.log(f"    Last-Modified       : {obj['LastModified']}")
        t.log(f"    Content-Type        : {obj.get('ContentType')}")
        size_ok = remote_size == local_size
        t.log(f"    размеры совпадают   : {'ДА' if size_ok else 'НЕТ'}")

        # Контрольное чтение первых байт прямо из хранилища.
        try:
            first = client.get_object(Bucket=settings.s3_bucket, Key=key, Range="bytes=0-255")
            head_bytes = first["Body"].read(256)
            t.log(f"    первые байты из S3  : {head_bytes[:120]!r}")
            with local_path.open("rb") as f:
                local_head = f.read(256)
            t.log(f"    локальные первые    : {local_head[:120]!r}")
            t.log(f"    содержимое совпадает: {'ДА' if head_bytes == local_head else 'НЕТ'}")
        except Exception as e:  # noqa: BLE001
            t.log(f"    контрольное чтение не удалось: {e}")

        # Список объектов в бакете.
        objects = client.list_objects_v2(Bucket=settings.s3_bucket).get("Contents", [])
        t.log(f"  всего объектов в бакете: {len(objects)}")
        for o in objects:
            t.log(f"    - {o['Key']}  ({human(o['Size'])})")
        t.log()

        t.rule("ИТОГ ЭТАПА 2")
        if size_ok:
            t.log(f"  [OK] объект {key} лежит в бакете {settings.s3_bucket}, размер {human(remote_size)}")
        else:
            t.log("  [FAIL] размеры не совпадают")
        t.log(f"  транскрипт сохранён: {out_path}")

    return 0 if size_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
