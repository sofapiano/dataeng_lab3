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

Данные лежат в **именованном томе Docker** `infra_rustfs_data` (Linux-ФС внутри
Docker VM). Bind-mount с Windows-хоста (`D:\rustfs\disk1`) использовать нельзя:
Windows-ФС не поддерживает блокировки, которые RustFS берёт на диски, и
хранилище начинает отдавать `503 SlowDownRead` (потеря кворума erasure-кода).
Учтите: смена тома означает потерю данных — том пересоздаётся вместе с
контейнером.

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
multipart-загрузкой (32 МБ на часть, 2 потока) и сверяет размер/содержимое.
Транскрипт: `reports/output/stage2_upload.txt`.

Повторная проверка без перезаливки: `python scripts/upload_dataset.py --verify`.

Проверка доступа DuckDB к этому объекту и побайтная сверка MD5:

```bash
python scripts/check_duckdb_s3.py
```

Транскрипт: `reports/output/check_duckdb_s3.txt`.

## Этапы 3–4: подсчёт навыков двумя способами и сверка

**Вариант 9.** Правило отбора и нормализация навыков заданы один раз в
`src/common/variant.py`; и MapReduce, и DuckDB используют именно эти
константы (для SQL предикат генерирует `filter_predicate_sql`), поэтому
расхождение результатов из-за разного фильтра невозможно.

### 3а. Собственный MapReduce поверх S3

```bash
python src/mapreduce/main.py
```

`src/mapreduce/`:

- `reader.py` — чтение объекта **байтовыми диапазонами** (`GetObject` +
  `Range`), границы чанков выравниваются по `\n`. Чанк владеет строками,
  которые начинаются в его диапазоне; хвост последней строки дочитывается
  отдельным Range-запросом. Тесты границ: `python tests/test_reader.py`;
- `mapper.py` — Map + Shuffle: CSV → фильтр варианта → нормализация навыков →
  ключи для k = 1, 2, 3 → разбиение по редьюсерам через `zlib.crc32`
  (встроенный `hash()` рандомизируется между процессами и не годится);
- `reducer.py` — Reduce: суммирование частичных частот;
- `main.py` — CLI, повтор упавших чанков, запись результатов.

Результаты пишутся в `data/results/mr/` и в бакет:
`results/mr/skills_k1.csv`, `skills_k2.csv`, `skills_k3.csv`,
`skill_histogram.csv`, `summary.csv`.

Отказоустойчивость (маппер убивает свой процесс на первой попытке чанка,
драйвер пересоздаёт пул и повторяет чанк):

```bash
python src/mapreduce/main.py --fail-chunk 6 --fail-mode exit \
    --tag _failure --no-upload --out reports/output/stage3_mapreduce_failure.txt
```

Итог на этом датасете: 4 176 вакансий варианта, 3 093 с навыками;
1 138 навыков, 14 089 пар, 73 431 тройка.

### 3б. DuckDB по тем же данным

```bash
python scripts/run_duckdb.py
```

SQL: `src/sql/skills_counts.sql` — `read_csv` прямо из `s3://hh-lake/...`,
`unnest(string_split(...))` + `lower(trim(...))` + `DISTINCT (vacancy_id, skill)`,
пары и тройки — self-join с `a.skill < b.skill[< c.skill]`.
Результаты: `data/results/duckdb/` и `results/duckdb/` в бакете.

### 4. Верификация

```bash
python scripts/verify.py
```

Скачивает оба набора результатов **из хранилища** и сверяет их
`FULL OUTER JOIN` по каждому k (`src/sql/verify_mapreduce_vs_duckdb.sql`).
Ожидаемый и фактический результат — 0 ключей только в MapReduce,
0 только в DuckDB, 0 расхождений частот.

## Конфигурация

`configs/pipeline.env` (создаётся из `configs/pipeline.example.env`) хранит
endpoint, ключи, имя бакета, ключи объектов и параметры MapReduce.
Переменные окружения имеют приоритет над файлом.

## Ограничения памяти

DuckDB по умолчанию резервирует ~80% физической памяти и падает на старте с
`Memory allocation still failed`. Поэтому лимиты заданы явно в
`configs/pipeline.env`:

```
DUCKDB_MEMORY_LIMIT=1GB
DUCKDB_THREADS=2
```

Скрипты, работающие с DuckDB, дополнительно выставляют
`OPENBLAS_NUM_THREADS=1` **до** импорта `duckdb`.
