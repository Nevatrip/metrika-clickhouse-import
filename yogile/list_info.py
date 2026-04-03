#!/usr/bin/env python3
"""
Выводит список стикеров, досок и проектов из Yogile.
Используется для получения UUID, которые нужно прописать в .env.

Запуск:
    python3 list_info.py                     # всё
    python3 list_info.py -p "Название"       # только один проект и его стикеры
    python3 list_info.py -p <project-uuid>   # то же самое по ID
"""

import argparse

from helpers.api import YogileApiClient

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--project', default=None,
                    help='Название или UUID проекта для фильтрации')
parser.add_argument('-t', '--task', default=None,
                    help='UUID карточки — показать её стикеры с названиями и значениями')
args = parser.parse_args()

api = YogileApiClient()

# ---------------------------------------------------------------------------
# --task mode: inspect a single card's stickers
# ---------------------------------------------------------------------------

if args.task:
    task = api.fetch_task(args.task)
    stickers = task.get('stickers') or {}

    print(f"\n  [{task.get('title', '?')}]")
    print(f"  ID:            {task['id']}")
    print(f"  idTaskProject: {task.get('idTaskProject', '')}")
    print()
    print('  STICKERS:')

    if not stickers:
        print('    (нет стикеров)')
    else:
        for sid, value in stickers.items():
            # Try to resolve name via string-sticker endpoint
            try:
                raw = api.fetch_string_sticker(sid)
                name = raw.get('name', '?')
                states = {s['id']: s['name'] for s in raw.get('states', []) if not s.get('deleted')}
                if states:
                    resolved = states.get(value, value)
                    print(f"    [{name}]  тип: выбор из списка")
                    print(f"      UUID стикера: {sid}")
                    print(f"      Значение:     {resolved!r}  (state_id: {value})")
                else:
                    print(f"    [{name}]  тип: свободное поле")
                    print(f"      UUID стикера: {sid}")
                    print(f"      Значение:     {value!r}")
            except Exception:
                print(f"    [?]  (не string-sticker)")
                print(f"      UUID стикера: {sid}")
                print(f"      Значение:     {value!r}")
    print()
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# Fetch everything
# ---------------------------------------------------------------------------

projects = api.fetch_projects()
string_stickers = api.fetch_paginated('/api-v2/string-stickers')
sprint_stickers = api.fetch_paginated('/api-v2/sprint-stickers')
boards = api.fetch_boards()

# ---------------------------------------------------------------------------
# Apply project filter
# ---------------------------------------------------------------------------

filter_project_id: str | None = None

if args.project:
    needle = args.project.strip()
    for p in projects:
        if p.get('deleted'):
            continue
        if p['id'] == needle or p['title'] == needle:
            filter_project_id = p['id']
            break
    if filter_project_id is None:
        print(f"Проект '{needle}' не найден. Доступные проекты:")
        for p in projects:
            if not p.get('deleted'):
                print(f"  [{p['title']}]  {p['id']}")
        raise SystemExit(1)

filtered_boards = [
    b for b in boards
    if not b.get('deleted') and (
        filter_project_id is None or b.get('projectId') == filter_project_id
    )
]

# Sticker UUIDs used in the filtered boards
board_sticker_ids: set[str] | None = None
if filter_project_id is not None:
    board_sticker_ids = set()
    for b in filtered_boards:
        board_sticker_ids.update((b.get('stickers') or {}).get('custom') or {})

# ---------------------------------------------------------------------------
# Build lookups
# ---------------------------------------------------------------------------

sticker_info: dict[str, str] = {}
for s in string_stickers:
    states = [st for st in s.get('states', []) if not st.get('deleted')]
    t = 'выбор из списка' if states else 'свободное поле'
    sticker_info[s['id']] = f"{s['name']} [{t}]"
for s in sprint_stickers:
    sticker_info[s['id']] = f"{s['name']} [спринт]"

project_titles: dict[str, str] = {p['id']: p['title'] for p in projects}

# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

print('=' * 60)
print('PROJECTS (проекты)')
print('=' * 60)

for p in projects:
    if p.get('deleted'):
        continue
    if filter_project_id and p['id'] != filter_project_id:
        continue
    print(f"\n  [{p['title']}]")
    print(f"    ID: {p['id']}")

# ---------------------------------------------------------------------------
# String stickers
# ---------------------------------------------------------------------------

print()
print('=' * 60)
print('STRING STICKERS')
print('=' * 60)

for s in string_stickers:
    if s.get('deleted'):
        continue
    if board_sticker_ids is not None and s['id'] not in board_sticker_ids:
        continue
    states = [st for st in s.get('states', []) if not st.get('deleted')]
    sticker_type = 'выбор из списка' if states else 'свободное поле'
    values = ', '.join(st['name'] for st in states) if states else None
    print(f"\n  [{s['name']}]  тип: {sticker_type}")
    print(f"    ID:     {s['id']}")
    print(f"    Иконка: {s.get('icon', '')}")
    if values:
        print(f"    Значения: {values}")

# ---------------------------------------------------------------------------
# Sprint stickers
# ---------------------------------------------------------------------------

print()
print('=' * 60)
print('SPRINT STICKERS')
print('=' * 60)

for s in sprint_stickers:
    if s.get('deleted'):
        continue
    if board_sticker_ids is not None and s['id'] not in board_sticker_ids:
        continue
    states = [st for st in s.get('states', []) if not st.get('deleted')]
    state_names = [st['name'] for st in states]
    print(f"\n  [{s['name']}]  тип: спринт")
    print(f"    ID:      {s['id']}")
    if state_names:
        print(f"    Спринты: {', '.join(state_names)}")

# ---------------------------------------------------------------------------
# Boards
# ---------------------------------------------------------------------------

print()
print('=' * 60)
print('BOARDS (доски)')
print('=' * 60)

for b in filtered_boards:
    custom_ids = list((b.get('stickers') or {}).get('custom') or {})
    project_title = project_titles.get(b.get('projectId', ''), b.get('projectId', ''))
    print(f"\n  [{b['title']}]")
    print(f"    ID:      {b['id']}")
    print(f"    Проект:  {project_title}")
    if custom_ids:
        print(f"    Стикеры:")
        for sid in custom_ids:
            if sid in sticker_info:
                print(f"      - {sticker_info[sid]}  ({sid})")
            else:
                # Стикер не попал в общий список — пробуем получить напрямую
                try:
                    raw = api.fetch_string_sticker(sid)
                    states = [st for st in raw.get('states', []) if not st.get('deleted')]
                    t = 'выбор из списка' if states else 'свободное поле'
                    label = f"{raw['name']} [{t}]"
                    deleted_mark = '  [DELETED]' if raw.get('deleted') else ''
                    sticker_info[sid] = label  # кэшируем
                    print(f"      - {label}{deleted_mark}  ({sid})")
                except Exception as e:
                    print(f"      - ? (не удалось получить: {e})  ({sid})")

print()
