#!/usr/bin/env python3

from datetime import datetime, date, timezone

import clickhouse_connect as cc
from clickhouse_driver import Client

import helpers.env as env
from helpers.api import YogileApiClient
from helpers.env import env_value_or_error, env_value_or_default

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def log(s: str) -> None:
    print(datetime.now().isoformat(), s)

ch_query = Client(
    host=env_value_or_error(env.CLICKHOUSE_HOST),
    user=env_value_or_error(env.CLICKHOUSE_USER),
    password=env_value_or_error(env.CLICKHOUSE_PASSWORD),
)

insert_client = cc.get_client(
    host=env_value_or_error(env.CLICKHOUSE_HOST),
    username=env_value_or_error(env.CLICKHOUSE_USER),
    password=env_value_or_error(env.CLICKHOUSE_PASSWORD),
)

db = env_value_or_error(env.CLICKHOUSE_DATABASE)
api = YogileApiClient()

snapshot_time = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

# ---------------------------------------------------------------------------
# Sprint number
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())

def calc_sprint_number(snap: datetime, sprint_start: date, sprint_length: int) -> int:
    delta = snap.date() - sprint_start
    if delta.days < 0:
        return 0
    return delta.days // sprint_length + 1

sprint_start = _parse_date(env_value_or_error(env.YOGILE_SPRINT_START))
sprint_length = int(env_value_or_default(env.YOGILE_SPRINT_LENGTH_DAYS, '14'))
sprint_number = calc_sprint_number(snapshot_time, sprint_start, sprint_length)

cards_from = _parse_date(env_value_or_error(env.YOGILE_CARDS_FROM))

allowed_projects: set[str] = {
    p.strip()
    for p in env_value_or_error(env.YOGILE_PROJECTS).split(',')
    if p.strip()
}

# ---------------------------------------------------------------------------
# Sticker UUIDs
# ---------------------------------------------------------------------------

sp_fe_uuid = env_value_or_error(env.YOGILE_STICKER_SP_FRONTEND)
sp_be_uuid = env_value_or_error(env.YOGILE_STICKER_SP_BACKEND)
project_uuid = env_value_or_error(env.YOGILE_STICKER_PROJECT)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(val: object, default: float = 0.0) -> float:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------------------
# Step 1: Users
# ---------------------------------------------------------------------------

log('Fetching users...')
users_raw = api.fetch_users()
user_rows = [
    (u['id'], u.get('realName') or u.get('email', ''))
    for u in users_raw
]
if user_rows:
    insert_client.insert(
        f'{db}.yogile_users',
        user_rows,
        column_names=['id', 'name'],
    )
log(f'  Upserted {len(user_rows)} users')

# ---------------------------------------------------------------------------
# Step 2: Boards + Columns → allowed_column_ids
# ---------------------------------------------------------------------------

log('Fetching boards...')
boards_raw = api.fetch_boards()
allowed_board_ids: set[str] = {
    b['id'] for b in boards_raw
    if not b.get('deleted') and b.get('projectId') in allowed_projects
}
log(f'  Found {len(allowed_board_ids)} boards in allowed projects')

log('Fetching columns...')
columns_raw = api.fetch_columns()
column_rows = [
    (c['id'], c.get('title', ''), c.get('boardId', ''))
    for c in columns_raw
    if not c.get('deleted')
]
if column_rows:
    insert_client.insert(
        f'{db}.yogile_columns',
        column_rows,
        column_names=['id', 'name', 'board_id'],
    )
log(f'  Upserted {len(column_rows)} columns')

allowed_column_ids: set[str] = {
    c['id'] for c in columns_raw
    if not c.get('deleted') and c.get('boardId') in allowed_board_ids
}
log(f'  Found {len(allowed_column_ids)} columns in allowed projects')

# ---------------------------------------------------------------------------
# Step 3: Project sticker states
# ---------------------------------------------------------------------------

log('Fetching project sticker states...')
project_sticker = api.fetch_string_sticker(project_uuid)
project_states: dict[str, str] = {
    s['id']: s['name']
    for s in project_sticker.get('states', [])
    if not s.get('deleted')
}
log(f'  Found {len(project_states)} project states')

# ---------------------------------------------------------------------------
# Step 4: Tasks → card snapshots
# ---------------------------------------------------------------------------

log('Fetching tasks...')
tasks_raw = api.fetch_tasks()
log(f'  Got {len(tasks_raw)} tasks')

# Load earliest known completion time per card
done_times: dict[str, datetime] = {
    row[0]: row[1]
    for row in ch_query.execute(
        f"SELECT id, min(snapshot_time) FROM {db}.yogile_cards WHERE completed = 1 GROUP BY id"
    )
}
log(f'  Found {len(done_times)} previously completed cards')

card_rows = []
skip_deleted = 0
skip_date = 0
skip_project = 0

for task in tasks_raw:
    if task.get('deleted'):
        skip_deleted += 1
        continue

    ts_ms = task.get('timestamp')
    if ts_ms is not None:
        created = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
        if created < cards_from:
            skip_date += 1
            continue

    stickers = task.get('stickers') or {}
    state_id = stickers.get(project_uuid, '')
    project_name = project_states.get(state_id, '') if state_id else ''

    if task.get('columnId') not in allowed_column_ids:
        skip_project += 1
        continue

    completed = int(bool(task.get('completed')))
    if completed:
        done_time = done_times.get(task['id'], snapshot_time)
    else:
        done_time = None

    card_rows.append((
        snapshot_time,
        sprint_number,
        task['id'],
        task.get('idTaskProject', ''),
        task.get('idTaskCommon', ''),
        task.get('columnId', ''),
        completed,
        done_time,
        project_name,
        _float(stickers.get(sp_fe_uuid)),
        _float(stickers.get(sp_be_uuid)),
        task.get('assigned') or [],
    ))

log(f'  Skipped: {skip_deleted} deleted, {skip_date} before {cards_from}, {skip_project} outside allowed projects')

# Upsert titles (separate lookup table)
title_rows = [
    (task['id'], task.get('title', ''))
    for task in tasks_raw
    if not task.get('deleted') and task.get('columnId') in allowed_column_ids
]
if title_rows:
    insert_client.insert(
        f'{db}.yogile_card_titles',
        title_rows,
        column_names=['id', 'title'],
    )

if card_rows:
    insert_client.insert(
        f'{db}.yogile_cards',
        card_rows,
        column_names=[
            'snapshot_time', 'sprint_number', 'id',
            'task_id', 'task_id_common', 'column_id', 'completed', 'done_time', 'project_name',
            'sp_frontend', 'sp_backend', 'assignee_ids',
        ],
    )

log(f'Inserted {len(card_rows)} card snapshots at {snapshot_time} (sprint {sprint_number})')
