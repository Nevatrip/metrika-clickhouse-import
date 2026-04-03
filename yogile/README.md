# Yogile -> ClickHouse ELM

Делает снапшоты карточек из YouGile в ClickHouse для построения аналитики. Каждый запуск добавляет новую порцию строк — append-only, без дедупликации.

## Быстрый старт локально

```bash
cp .env.example .env  # заполнить переменные
python3 list_info.py  # найти UUID стикеров и проектов
python3 init.py       # создать таблицы (один раз)
python3 insert.py     # запустить импорт
```

### Где взять YOGILE_API_TOKEN

Описано тут: https://ru.yougile.com/api-v2

### Где взять UUID стикеров и проектов

Запустить `python3 list_info.py`. Выведет все проекты, стикеры с типами и доски.

Для конкретного проекта:
```bash
python3 list_info.py -p "Название проекта"
```

Если стикер на карточке есть, но название не определяется — можно проверить по конкретной карточке:
```bash
python3 list_info.py --task <uuid-карточки>
```

## Таблицы

### `yogile_cards`
Снапшоты карточек. Одна строка = одна карточка в один момент времени.

Ключевые поля: `snapshot_time`, `sprint_number`, `id`, `title`, `task_id` (напр. `TECH-1834`), `task_id_common` (напр. `ID-484`), `column_id`, `completed`, `project_name`, `sp_frontend`, `sp_backend`, `assignee_ids`.

---

### `yogile_users`
Справочник исполнителей. Обновляется при каждом запуске.

Поля: `id`, `name`. Используется для расшифровки `assignee_ids` в карточках.

---

### `yogile_columns`
Справочник колонок досок. Обновляется при каждом запуске.

Поля: `id`, `name`, `board_id`. Используется для расшифровки `column_id` в карточках.

## Полезные запросы

История карточки по снапшотам:
```sql
SELECT snapshot_time, sprint_number, title, col.name AS column, completed, sp_frontend, sp_backend
FROM yogile.yogile_cards c
LEFT JOIN yogile.yogile_columns col ON c.column_id = col.id
WHERE task_id = 'TECH-1834'
ORDER BY snapshot_time;
```

Состояние всех карточек на последнем снапшоте:
```sql
SELECT *
FROM yogile.yogile_cards
WHERE snapshot_time = (SELECT max(snapshot_time) FROM yogile.yogile_cards)
ORDER BY project_name, task_id;
```
