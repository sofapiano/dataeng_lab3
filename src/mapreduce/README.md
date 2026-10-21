# MapReduce (custom)

Здесь должна быть ваша реализация chunk-based чтения из объектного хранилища и подсчётов:
- отдельных навыков;
- пар навыков;
- троек навыков.

Рекомендуемые файлы:
- `reader.py` — чтение JSONL чанками из S3-compatible storage;
- `mapper.py` — генерация ключей (skill, pair, triple);
- `reducer.py` — агрегация частот;
- `main.py` — CLI-точка входа.
