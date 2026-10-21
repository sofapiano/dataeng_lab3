"""Проверка доступа DuckDB к объекту в RustFS через httpfs.

Это не основной этап, а техническая проверка этапа 1: если DuckDB не умеет
читать объект из хранилища, весь дальнейший SQL-подход бессмыслен.

Запуск:
    python scripts/check_duckdb_s3.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# DuckDB тянет за собой OpenBLAS, который на старте резервирует большой буфер
# под каждый поток. На этой машине свободно меньше 1 ГБ RAM, и без ограничения
# импорт падает с "Memory allocation still failed". Ставим переменную ДО
# импорта duckdb - иначе она не подхватится.
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb  # noqa: E402

from common.config import load_settings  # noqa: E402
from common.hashing import md5_file, md5_stream  # noqa: E402
from common.reporting import Transcript  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402


def main() -> int:
    settings = load_settings()
    out_path = Path(settings.report_dir) / "check_duckdb_s3.txt"
    uri = settings.input_uri

    with Transcript(out_path) as t:
        t.rule("ПРОВЕРКА: ЧТЕНИЕ ОБЪЕКТА ИЗ RUSTFS ЧЕРЕЗ DUCKDB httpfs")
        t.log()
        t.log(f"  URI объекта : {uri}")
        t.log()

        con = duckdb.connect(
            config={
                "memory_limit": settings.duckdb_memory_limit,
                "threads": settings.duckdb_threads,
            }
        )
        con.execute("INSTALL httpfs; LOAD httpfs;")
        t.log("  [OK] httpfs установлен и загружен")
        t.log(f"       memory_limit={settings.duckdb_memory_limit}, "
              f"threads={settings.duckdb_threads} (OpenBLAS_NUM_THREADS=1)")
        t.log()

        client = make_s3_client(settings)

        # Настройки доступа к RustFS (path-style, без SSL, локальный endpoint).
        con.execute(f"""
            SET s3_endpoint='{settings.s3_endpoint.replace("http://", "")}';
            SET s3_access_key_id='{settings.s3_access_key}';
            SET s3_secret_access_key='{settings.s3_secret_key}';
            SET s3_region='{settings.s3_region}';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
        """)
        t.log("  [OK] параметры подключения к S3 выставлены:")
        t.log(f"       s3_endpoint={settings.s3_endpoint.replace('http://', '')}, "
              f"s3_url_style=path, s3_use_ssl=false")
        t.log()

        t.rule("1. Список бакетов (через S3 API)")
        try:
            names = [b["Name"] for b in client.list_buckets()["Buckets"]]
            t.log(f"  бакеты: {names}")
        except Exception as e:  # noqa: BLE001
            t.log(f"  не удалось получить список бакетов: {e}")
        t.log()

        t.rule("2. Чтение первых строк CSV прямо из S3")
        try:
            df = con.execute(
                f"SELECT source, vacancy_id, name, role, area FROM read_csv('{uri}', "
                f"header=true, all_varchar=true) LIMIT 5"
            ).fetchdf()
            t.log(df.to_string(index=False, max_colwidth=45))
            t.log("  [OK] DuckDB читает объект из RustFS")
        except Exception as e:  # noqa: BLE001
            t.log(f"  ОШИБКА чтения: {type(e).__name__}: {e}")
            return 1
        t.log()

        t.rule("3. Сквозная проверка объекта: потоковое чтение из S3 и сверка с локальным файлом")
        try:
            local = Path(settings.local_input)
            local_md5, local_size, local_nl = md5_file(local), local.stat().st_size, None
            with local.open("rb") as f:
                _, _, local_nl = md5_stream(f)
            t.log(f"  локально : MD5={local_md5}  байт={local_size}  строк(\\n)={local_nl}")
        except Exception as e:  # noqa: BLE001
            t.log(f"  не удалось посчитать локальный файл: {e}")
            return 1

        try:
            resp = client.get_object(Bucket=settings.s3_bucket, Key=settings.input_object)
            with resp["Body"] as body:
                remote_md5, remote_size, remote_nl = md5_stream(body)
            t.log(f"  в S3     : MD5={remote_md5}  байт={remote_size}  строк(\\n)={remote_nl}")
        except Exception as e:  # noqa: BLE001
            t.log(f"  ОШИБКА чтения объекта из S3: {type(e).__name__}: {e}")
            return 1

        md5_ok = local_md5 == remote_md5
        size_ok = local_size == remote_size
        nl_ok = local_nl == remote_nl
        t.log(f"  контрольная сумма совпадает : {'ДА' if md5_ok else 'НЕТ'}")
        t.log(f"  размер совпадает             : {'ДА' if size_ok else 'НЕТ'}")
        t.log(f"  число строк совпадает        : {'ДА' if nl_ok else 'НЕТ'}")
        if not (md5_ok and size_ok and nl_ok):
            return 1
        t.log(f"  [OK] объект в хранилище побайтно идентичен локальному файлу")
        t.log()
        t.log("  Примечание: полный count(*) по объекту в DuckDB на этой машине требует")
        t.log(f"  явного лимита памяти (memory_limit={settings.duckdb_memory_limit}),")
        t.log("  иначе движок падает на старте при свободной памяти <1 ГБ.")
        t.log()
        t.log(f"  транскрипт сохранён: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
