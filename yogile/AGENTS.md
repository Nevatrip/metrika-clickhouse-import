# AGENTS — Yogile ETL

Инструкции для AI-агента, работающего с этим модулем.

## Паттерны, которые нужно соблюдать

**env.py** — никогда не читай переменные напрямую через `os.getenv`. Всегда через `env_value_or_error(key)` или `env_value_or_default(key, default)`. Константы-ключи объявлены в `helpers/env.py`.

**Два клиента ClickHouse** — всегда оба, как в `insert.py`:
- `clickhouse_connect` — для `insert_client.insert(...)`
- `clickhouse_driver` — для DDL и SELECT (`ch_query.execute(...)`)

**Логирование** — через `log(s: str)` с timestamp, не через `print` напрямую.

**Пагинация API** — ответ всегда `{"paging": {"next": bool, ...}, "content": [...]}`. Используй `api.fetch_paginated(path)`.

## Структура стикеров

Стикеры на карточке: `task["stickers"] = {sticker_uuid: value}`.

- Стикер-выбор (project): `value` — это ID состояния → расшифровывается через `project_states` dict, который строится один раз через `api.fetch_string_sticker(uuid)`.
- Стикер-число (SP): `value` — строка с числом, напр. `"8"` → `float(value)`. Fallback `0.0`.

UUID стикеров берутся из `.env`, не хардкодятся.

## Как проверить изменения

```bash
cd yogile
python3 init.py    # убедиться что таблицы создаются без ошибок
python3 insert.py  # должно вывести "Inserted N card snapshots at ..."
```

Для разведки API (найти UUID стикеров, проектов, досок):
```bash
python3 list_info.py
```

## Чего не делать

- Не добавляй `OPTIMIZE TABLE FINAL` для `yogile_cards` — это не ReplacingMergeTree.
- Не меняй ENGINE таблиц в `init.py` на ReplacingMergeTree для `yogile_cards`.
- Не фильтруй `archived == true` — архивные карточки нужны для истории.
- Не добавляй checkpoint-логику — здесь не инкрементальный sync, а полный снапшот каждый раз.
- `YOGILE_PROJECTS` — обязательная переменная, не делай её опциональной. Содержит UUID проектов из `/api-v2/projects` (не названия из стикера). Фильтрация идёт по иерархии: проект → доски → колонки → задача.
