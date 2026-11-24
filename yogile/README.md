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

Ключевые поля: `snapshot_time`, `sprint_number`, `id`, `title`, `task_id` (напр. `TECH-1834`), `task_id_common` (напр. `ID-484`), `column_id`, `completed`, `project_name`, `sp_frontend`, `sp_backend`, `assignee_ids`, `task_type`, `test_mark`.

`task_type` — значение стикера «Тип задачи» (`Баг`, `Плановая задача`, `Оперативная задача`…), `test_mark` — значение стикера «Тест» (`Тест: критично`, `Тест: после релиза`, `Тест: в отдельную задачу`). UUID стикеров задаются в `YOGILE_STICKER_TASK_TYPE` и `YOGILE_STICKER_TEST`; если переменная пустая, колонка не заполняется. В снапшотах до появления колонок поля пустые — актуальное значение брать из последнего снапшота: `argMax(task_type, snapshot_time)`.

---

### `yogile_task_links`
Связи «родитель → подзадача» для карточек на досках и подзадач без колонки. Связи накапливаются: если подзадачу отвязали, старая связь остаётся.

Поля: `parent_id`, `child_id` (UUID карточек), `seen_at` — когда связь видели последний раз.

---

### `yogile_subtasks`
Подзадачи, которые не лежат ни в одной колонке (например, баги внутри карточки «Тестирование …»). В `yogile_cards` их нет, потому что выгрузка идёт по колонкам. Хранится последнее состояние, без снапшотов.

Поля: `id`, `task_id` (напр. `TECH-1648`), `title`, `created_at`, `completed`, `archived`, `task_type`, `test_mark`, `fetched_at`.

Каждая такая подзадача запрашивается из API отдельно, поэтому первый запуск занимает дольше обычного (около 1,5–2 тыс. запросов, 10–15 минут). Дальше перезапрашиваются только новые подзадачи и незавершённые, созданные за последние `YOGILE_SUBTASKS_REFRESH_DAYS` дней (по умолчанию 30).

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

Баги, найденные на тесте (актуальные стикеры по последнему снапшоту):
```sql
SELECT task_id, argMax(task_type, snapshot_time) AS type, argMax(test_mark, snapshot_time) AS test
FROM yogile.yogile_cards
GROUP BY task_id
HAVING type = 'Баг' AND test != '';
```

Подзадачи карточки «Тестирование …» (один уровень вложенности):
```sql
SELECT s.task_id, s.title, s.task_type, s.test_mark, s.completed
FROM yogile.yogile_task_links AS l FINAL
JOIN yogile.yogile_card_titles AS p ON p.id = l.parent_id
JOIN yogile.yogile_subtasks AS s FINAL ON s.id = l.child_id
WHERE p.title ILIKE 'Тестирование%';
```
