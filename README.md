# dataeng_lab3

Репозиторий для лабораторной/домашней работы по инженерии данных.

**Вариант 9 — «Мобильная разработка».** Подробное описание варианта, правило
отбора вакансий и правила нормализации навыков — в `src/common/variant.py`
(единый источник правды для MapReduce и DuckDB).

## Технический каркас

- `infra/` — запуск объектного хранилища (RustFS) через Docker Compose;
- `configs/` — конфигурация пайплайна (шаблон `pipeline.example.env`, рабочая копия `pipeline.env` в `.gitignore`);
- `src/common/` — общие модули: конфигурация, S3-клиент, правило варианта, отчёты;
- `src/mapreduce/` — собственная реализация MapReduce (этап 3);
- `src/sql/` — SQL-запросы для DuckDB и сверки результатов (этап 3);
- `scripts/` — загрузка данных, запуск и профилирование;
- `data/` — каталоги для входных/промежуточных/итоговых данных;
- `profiling/` — результаты замеров времени и памяти;
- `reports/output/` — текстовые транскрипты запусков для отчёта.

## Этапы 1–2: хранилище и данные

### Поднять хранилище

```bash
cd infra
docker compose up -d
docker compose ps
```

- S3 API: <http://localhost:9000>
- Веб-консоль: <http://localhost:9001/rustfs/console/> (в RustFS 1.0.1 консоль живёт под префиксом `/rustfs/console/`)
- Учётные данные: `rustfsadmin` / `rustfsadmin` (см. `infra/.env`)

Данные дисков лежат на хосте в `D:\rustfs\disk1..4` (bind mounts), поэтому
пересоздание контейнера их не теряет.

### Создать бакет `hh-lake`

```bash
python scripts/setup_storage.py
```

Скрипт идемпотентен: проверяет эндпоинт, консоль, создаёт бакет `hh-lake`
и печатает список бакетов. Транскрипт: `reports/output/stage1_storage.txt`.

### Загрузить датасет

```bash
python scripts/upload_dataset.py
```

Кладёт `data/raw/it_vacancies_2024.csv` (808 МБ) в `s3://hh-lake/raw/it_vacancies_2024.csv`
multipart-загрузкой (64 МБ на часть, 4 потока) и сверяет размер/содержимое.
Транскрипт: `reports/output/stage2_upload.txt`.

Проверка доступа DuckDB к этому объекту и побайтная сверка MD5:

```bash
python scripts/check_duckdb_s3.py
```

Транскрипт: `reports/output/check_duckdb_s3.txt`.

## Конфигурация

`configs/pipeline.env` (создаётся из `configs/pipeline.example.env`) хранит
endpoint, ключи, имя бакета, ключи объектов и параметры MapReduce.
Переменные окружения имеют приоритет над файлом.
