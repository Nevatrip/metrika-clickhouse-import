# GitHub -> ClickHouse

Инкрементально выгружает коммиты ветки `main` (first-parent цепочку) и списки изменённых файлов из GitHub в ClickHouse. Нужно для статистики багов: `fix:` / `fix(TECH-xxxx):` / `feat(TECH-xxxx):` коммиты в `main` + связь с карточками YouGile по `task_id`.

## Быстрый старт локально

```bash
cp .env.example .env  # заполнить токен
python3 init.py       # создать таблицы (один раз)
python3 insert.py     # запустить импорт
```

Первый запуск загружает историю с `GITHUB_COMMITS_FROM`. Если коммитов больше, чем `GITHUB_MAX_DETAIL_CALLS`, догрузка продолжится в следующих запусках (скрипт сохраняет прогресс по мере работы).

### Токен

Fine-grained personal access token (Settings → Developer settings → Fine-grained tokens):
- Resource owner: `Nevatrip`
- Repository access: репозитории из `GITHUB_REPOS`
- Permissions: **Contents: Read-only** (Metadata: Read-only добавится сам)

## Таблицы

### `commits`
Одна строка = один коммит в first-parent цепочке ветки (squash-merge PR или прямой коммит).

| поле | описание |
|---|---|
| `repo`, `sha` | ключ |
| `committed_at` | дата коммита в ветку (UTC) |
| `subject`, `message` | первая строка / полное сообщение |
| `prefix` | тип по Conventional Commits в нижнем регистре: `fix`, `feat`, `chore`… |
| `scope` | содержимое скобок: `TECH-2903`, `api`… |
| `task_id` | `TECH-xxxx` из scope или из первой строки, иначе `''` |
| `parents_count` | `> 1` — merge-коммит (ветка влита без squash) |

### `commit_files`
Изменённые файлы коммита: `repo`, `sha`, `filename`, `status`, `additions`, `deletions`. GitHub отдаёт максимум 300 файлов на коммит.

Обе таблицы `ReplacingMergeTree` — при выборках использовать `FINAL` или `DISTINCT`.

## Полезные запросы

Фиксы в main по неделям:
```sql
SELECT toStartOfWeek(committed_at) AS week, repo, count() AS fixes
FROM github.commits FINAL
WHERE prefix = 'fix'
GROUP BY week, repo
ORDER BY week;
```

`feat(TECH-…)` коммиты, у которых карточка — баг:
```sql
SELECT c.repo, c.sha, c.subject, y.task_type
FROM github.commits c FINAL
JOIN (
    SELECT task_id, argMax(task_type, snapshot_time) AS task_type
    FROM yogile.yogile_cards GROUP BY task_id
) y ON y.task_id = c.task_id
WHERE c.prefix = 'feat' AND y.task_type = 'Баг';
```
