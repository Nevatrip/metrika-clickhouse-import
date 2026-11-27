#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone

import clickhouse_connect as cc
from clickhouse_driver import Client

import helpers.env as env
from helpers.api import BitrixApiClient
from helpers.env import env_value_or_error
import json

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def log_func(s: str):
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

api = BitrixApiClient(env_value_or_error(env.BITRIX_WEBHOOK_URL))

# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint() -> dict:
    rows = ch_query.execute(
        f"SELECT entity, synced_at FROM {db}.bitrix_checkpoint FINAL"
    )
    return {entity: synced_at.isoformat() for entity, synced_at in rows}

def save_checkpoint(entities: list[str], synced_at: str) -> None:
    dt = datetime.fromisoformat(synced_at)
    insert_client.insert(
        f"{db}.bitrix_checkpoint",
        [(e, dt) for e in entities],
        column_names=['entity', 'synced_at'],
    )

def filter_dt(checkpoint: dict, key: str) -> str | None:
    val = checkpoint.get(key)
    if not val:
        return None
    dt = datetime.fromisoformat(val) - timedelta(minutes=5)
    return dt.strftime('%Y-%m-%dT%H:%M:%S')

def ensure_uf_columns(entity_type: str, fields_meta: dict[str, str]) -> None:
    """fields_meta: {field_id: human_readable_label}"""
    table = f"{db}.bitrix_{entity_type}s"
    existing = {row[0]: row[4] for row in ch_query.execute(f"DESCRIBE TABLE {table}")}

    def _esc(s: str) -> str:
        return s.replace("'", "''")

    ops = []
    for fid, label in fields_meta.items():
        comment = _esc(label) if label and label != fid else ''
        if fid not in existing:
            ops.append(f"ADD COLUMN IF NOT EXISTS {fid} String DEFAULT '' COMMENT '{comment}'")
        elif comment and not existing[fid]:
            ops.append(f"COMMENT COLUMN {fid} '{comment}'")

    if ops:
        ch_query.execute(f"ALTER TABLE {table} {', '.join(ops)}")
        log_func(f"UF schema updated ({len(ops)} ops) in {table}")

# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except (ValueError, TypeError):
        return None

def _int(val, default: int = 0) -> int:
    try:
        return int(val or default)
    except (ValueError, TypeError):
        return default

def _float(val, default: float = 0.0) -> float:
    try:
        return float(val or default)
    except (ValueError, TypeError):
        return default

def _str(val) -> str:
    return str(val) if val is not None else ''

def _str_uf(val) -> str:
    if val is None:
        return ''
    if isinstance(val, list):
        return ', '.join(str(v) for v in val if v is not None)
    return str(val)

def _first_value(field) -> str:
    if isinstance(field, list) and field:
        return _str(field[0].get('VALUE', ''))
    return ''

def transform_lead(r: dict, uf_fields: list[str] = []) -> tuple:
    return (
        _int(r.get('ID')),
        _str(r.get('TITLE')),
        _str(r.get('STATUS_ID')),
        _str(r.get('SOURCE_ID')),
        _parse_dt(r.get('DATE_CREATE')) or datetime(1970, 1, 1),
        _parse_dt(r.get('DATE_MODIFY')) or datetime(1970, 1, 1),
        _int(r.get('ASSIGNED_BY_ID')),
        _str(r.get('UTM_SOURCE')),
        _str(r.get('UTM_MEDIUM')),
        _str(r.get('UTM_CAMPAIGN')),
        _str(r.get('UTM_CONTENT')),
        _str(r.get('UTM_TERM')),
        _first_value(r.get('PHONE')),
        _first_value(r.get('EMAIL')),
        _float(r.get('OPPORTUNITY')),
        _str(r.get('CURRENCY_ID')),
        json.dumps(r, ensure_ascii=False),
        *(_str_uf(r.get(fid)) for fid in uf_fields),
    )

LEADS_COLUMNS = [
    'ID', 'TITLE', 'STATUS_ID', 'SOURCE_ID', 'DATE_CREATE', 'DATE_MODIFY',
    'ASSIGNED_BY_ID', 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT',
    'UTM_TERM', 'PHONE', 'EMAIL', 'OPPORTUNITY', 'CURRENCY_ID', 'raw_data',
]

def transform_deal(r: dict, uf_fields: list[str] = []) -> tuple:
    return (
        _int(r.get('ID')),
        _str(r.get('TITLE')),
        _str(r.get('STAGE_ID')),
        _str(r.get('TYPE_ID')),
        _str(r.get('SOURCE_ID')),
        _parse_dt(r.get('DATE_CREATE')) or datetime(1970, 1, 1),
        _parse_dt(r.get('DATE_MODIFY')) or datetime(1970, 1, 1),
        _int(r.get('ASSIGNED_BY_ID')),
        _int(r.get('CONTACT_ID')),
        _int(r.get('LEAD_ID')),
        _int(r.get('CATEGORY_ID')),
        _float(r.get('OPPORTUNITY')),
        _str(r.get('CURRENCY_ID')),
        _str(r.get('UTM_SOURCE')),
        _str(r.get('UTM_MEDIUM')),
        _str(r.get('UTM_CAMPAIGN')),
        _str(r.get('UTM_CONTENT')),
        _str(r.get('UTM_TERM')),
        json.dumps(r, ensure_ascii=False),
        *(_str_uf(r.get(fid)) for fid in uf_fields),
    )

DEALS_COLUMNS = [
    'ID', 'TITLE', 'STAGE_ID', 'TYPE_ID', 'SOURCE_ID', 'DATE_CREATE', 'DATE_MODIFY',
    'ASSIGNED_BY_ID', 'CONTACT_ID', 'LEAD_ID', 'CATEGORY_ID', 'OPPORTUNITY', 'CURRENCY_ID',
    'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT', 'UTM_TERM', 'raw_data',
]

def transform_status(r: dict, semantics: str = '') -> tuple:
    raw_sem = _str(r.get('SEMANTICS', semantics))
    # For category stages: derive semantics from STATUS_ID pattern (CX:WON / CX:LOSE)
    if not raw_sem:
        sid = _str(r.get('STATUS_ID', ''))
        if sid.endswith(':WON') or sid == 'WON':
            raw_sem = 'S'
        elif sid.endswith(':LOSE') or sid == 'LOSE' or sid.endswith(':APOLOGY') or sid == 'APOLOGY':
            raw_sem = 'F'
    return (
        _str(r.get('ENTITY_ID')),
        _str(r.get('STATUS_ID')),
        _str(r.get('NAME')),
        _int(r.get('SORT')),
        _str(r.get('COLOR')),
        raw_sem,
    )

STATUSES_COLUMNS = ['ENTITY_ID', 'STATUS_ID', 'NAME', 'SORT', 'COLOR', 'SEMANTICS']

def transform_history(r: dict, entity_type_id: int) -> tuple:
    return (
        _int(r.get('ID')),
        entity_type_id,
        _int(r.get('ENTITY_ID')),
        _str(r.get('STATUS_FROM_ID') or r.get('STATUS_FROM') or ''),
        _str(r.get('STATUS_TO_ID') or r.get('STATUS_TO') or ''),
        _parse_dt(r.get('CREATED_DATE') or r.get('CREATED_TIME')) or datetime(1970, 1, 1),
        _int(r.get('USER_ID') or r.get('MODIFIED_BY')),
    )

HISTORY_COLUMNS = [
    'ID', 'ENTITY_TYPE_ID', 'ENTITY_ID', 'STATUS_FROM_ID', 'STATUS_TO_ID',
    'CREATED_DATE', 'USER_ID',
]


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------

checkpoint = load_checkpoint()
run_start = datetime.utcnow().isoformat()

log_func(f"Bitrix24 import started. Checkpoint: {checkpoint}")

# 1. Statuses (full refresh every run)
statuses = api.fetch_statuses(log_func)
if statuses:
    insert_client.insert(
        f"{db}.bitrix_statuses",
        [transform_status(r) for r in statuses],
        column_names=STATUSES_COLUMNS,
    )
log_func(f"INSERTED {len(statuses)} statuses")

# 1b. Deal categories (full refresh)
categories = api.fetch_deal_categories(log_func)
# Add default category (ID=0) which API doesn't return
categories_rows = [(0, 'Основная воронка', 0)] + [
    (_int(r.get('ID')), _str(r.get('NAME')), _int(r.get('SORT')))
    for r in categories
]
insert_client.insert(
    f"{db}.bitrix_deal_categories",
    categories_rows,
    column_names=['ID', 'NAME', 'SORT'],
)
log_func(f"INSERTED {len(categories_rows)} deal categories")

# 1c. Deal category stages — includes system WON/LOSE stages (e.g. C13:WON)
all_category_ids = [row[0] for row in categories_rows]
stages_from_categories = []
for cat_id in all_category_ids:
    stages = api.fetch_deal_category_stages(cat_id, log_func)
    stages_from_categories.extend(stages)
if stages_from_categories:
    insert_client.insert(
        f"{db}.bitrix_statuses",
        [transform_status({**s, 'ENTITY_ID': 'DEAL_STAGE'}) for s in stages_from_categories],
        column_names=STATUSES_COLUMNS,
    )
log_func(f"INSERTED {len(stages_from_categories)} deal category stages")

# 2. Field metadata (full refresh)
fields_by_type: dict[str, dict] = {}
fields_rows = []
for entity_type in ('lead', 'deal'):
    fields = api.fetch_fields(entity_type, log_func)
    fields_by_type[entity_type] = fields
    for field_id, meta in fields.items():
        fields_rows.append((
            entity_type,
            field_id,
            _str(meta.get('formLabel') or meta.get('listLabel') or meta.get('title') or ''),
            _str(meta.get('type', '')),
            1 if meta.get('isMultiple') else 0,
            1 if meta.get('isRequired') else 0,
        ))
if fields_rows:
    insert_client.insert(
        f"{db}.bitrix_fields",
        fields_rows,
        column_names=['entity_type', 'field_id', 'title', 'field_type', 'is_multiple', 'is_required'],
    )
log_func(f"INSERTED {len(fields_rows)} field definitions")

# 2b. Enum values (full refresh)
enum_rows = []
for entity_type, fields in fields_by_type.items():
    for field_id, meta in fields.items():
        for item in meta.get('items') or []:
            try:
                enum_rows.append((entity_type, field_id, int(item['ID']), _str(item.get('VALUE', ''))))
            except (KeyError, ValueError, TypeError):
                pass
if enum_rows:
    insert_client.insert(
        f"{db}.bitrix_enum_values",
        enum_rows,
        column_names=['entity_type', 'field_id', 'item_id', 'item_value'],
    )
log_func(f"INSERTED {len(enum_rows)} enum values")

# 3. Ensure UF_* columns exist in leads/deals tables
def _uf_meta(entity_type: str) -> dict[str, str]:
    return {
        fid: _str(meta.get('formLabel') or meta.get('listLabel') or meta.get('title') or '')
        for fid, meta in fields_by_type.get(entity_type, {}).items()
        if fid.startswith('UF_')
    }

lead_uf_meta = _uf_meta('lead')
deal_uf_meta = _uf_meta('deal')
lead_uf_fields = sorted(lead_uf_meta)
deal_uf_fields = sorted(deal_uf_meta)
ensure_uf_columns('lead', lead_uf_meta)
ensure_uf_columns('deal', deal_uf_meta)
log_func(f"UF columns: {len(lead_uf_fields)} lead, {len(deal_uf_fields)} deal")

# 4. Leads
leads_from = filter_dt(checkpoint, 'leads')
leads = api.fetch_leads(leads_from, log_func)
if leads:
    insert_client.insert(
        f"{db}.bitrix_leads",
        [transform_lead(r, lead_uf_fields) for r in leads],
        column_names=LEADS_COLUMNS + lead_uf_fields,
    )
log_func(f"INSERTED {len(leads)} leads (from={leads_from})")

# 5. Deals
deals_from = filter_dt(checkpoint, 'deals')
deals = api.fetch_deals(deals_from, log_func)
if deals:
    insert_client.insert(
        f"{db}.bitrix_deals",
        [transform_deal(r, deal_uf_fields) for r in deals],
        column_names=DEALS_COLUMNS + deal_uf_fields,
    )
log_func(f"INSERTED {len(deals)} deals (from={deals_from})")

# 6. Stage history
history_from = filter_dt(checkpoint, 'stage_history')
history_rows = []
for entity_type, entity_type_id in [('lead', 1), ('deal', 2)]:
    records = api.fetch_stage_history(entity_type, history_from, log_func)
    history_rows.extend(transform_history(r, entity_type_id) for r in records)
if history_rows:
    insert_client.insert(
        f"{db}.bitrix_status_history",
        history_rows,
        column_names=HISTORY_COLUMNS,
    )
log_func(f"INSERTED {len(history_rows)} stage history records (from={history_from})")

# Save checkpoint (all entities updated at the very end)
save_checkpoint(['leads', 'deals', 'stage_history'], run_start)
log_func(f"CHECKPOINT SAVED: {run_start}")

# 7. Worktime (timeman). Перезаписываем последние месяцы целиком,
#    при пустой таблице — подтягиваем историю за WORKDAYS_HISTORY_MONTHS.
WORKDAYS_HISTORY_MONTHS = 12
WORKDAYS_REFRESH_MONTHS = 2

def _months_back(n: int) -> list[tuple[int, int]]:
    today = datetime.now()
    y, m = today.year, today.month
    res = []
    for _ in range(n):
        res.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return res

def _sec(val) -> int:
    return max(_int(val), 0)

def _local_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None

def transform_workday(d: dict, user: dict, departments: dict[int, str]) -> tuple | None:
    start = _local_dt(d.get('workday_date_start'))
    if start is None:
        return None
    complete = bool(d.get('workday_complete'))
    finish = _local_dt(d.get('workday_date_finish')) if complete else None
    duration = _sec(d.get('workday_duration'))
    pause = _sec(d.get('workday_time_leaks_user'))
    name = ' '.join(p for p in (_str(user.get('NAME')).strip(), _str(user.get('LAST_NAME')).strip()) if p)
    dep_ids = user.get('UF_DEPARTMENT') or []
    dep = ', '.join(departments.get(_int(i), '') for i in dep_ids if departments.get(_int(i)))
    return (
        _int(user.get('ID')),
        name,
        _str(user.get('WORK_POSITION')),
        dep,
        start.date(),  # ночную смену Битрикс отдаёт за оба дня — считаем её по дню начала
        start,
        finish,
        1 if complete else 0,
        duration,
        pause,
        max(duration - pause, 0),
        _sec(d.get('workday_duration_final')),
    )

WORKDAYS_COLUMNS = [
    'USER_ID', 'USER_NAME', 'WORK_POSITION', 'DEPARTMENT', 'WORK_DATE', 'DATE_START',
    'DATE_FINISH', 'IS_COMPLETE', 'DURATION_SEC', 'PAUSE_SEC', 'WORK_SEC', 'ACTIVE_SEC',
]

try:
    has_workdays = ch_query.execute(f"SELECT count() FROM {db}.bitrix_workdays")[0][0] > 0
    months = _months_back(WORKDAYS_REFRESH_MONTHS if has_workdays else WORKDAYS_HISTORY_MONTHS)
    users = api.fetch_users(log_func)
    departments = api.fetch_departments(log_func)
    workday_rows = []
    for user in users:
        for year, month in months:
            for d in api.fetch_workdays(_int(user.get('ID')), year, month):
                row = transform_workday(d, user, departments)
                if row:
                    workday_rows.append(row)
    # дубли ночных смен: одна смена приходит в отчёте и за день начала, и за следующий
    workday_rows = list({(r[0], r[4]): r for r in workday_rows}.values())
    if workday_rows:
        insert_client.insert(f"{db}.bitrix_workdays", workday_rows, column_names=WORKDAYS_COLUMNS)
    # чистим дубли, записанные раньше по дате отчёта, а не по дате начала смены
    ch_query.execute(
        f"ALTER TABLE {db}.bitrix_workdays DELETE WHERE WORK_DATE != toDate(DATE_START)",
        settings={'mutations_sync': 1},
    )
    log_func(f"INSERTED {len(workday_rows)} workdays ({len(users)} users, months={months})")
except Exception as exc:
    log_func(f"WORKDAYS IMPORT FAILED: {exc}")

# 8. Task time tracking (таймер в задачах). Записей мало — грузим все целиком.
try:
    elapsed = api.fetch_task_elapsed(log_func)
    task_titles = api.fetch_task_titles(sorted({_int(e.get('TASK_ID')) for e in elapsed}), log_func)
    user_names = {
        _int(u.get('ID')): ' '.join(p for p in (_str(u.get('NAME')).strip(), _str(u.get('LAST_NAME')).strip()) if p)
        for u in api.fetch_users()
    } if elapsed else {}
    task_time_rows = []
    for e in elapsed:
        created = _local_dt(e.get('CREATED_DATE'))
        if created is None:
            continue
        task_id, user_id = _int(e.get('TASK_ID')), _int(e.get('USER_ID'))
        task_time_rows.append((
            _int(e.get('ID')), task_id, task_titles.get(task_id, ''),
            user_id, user_names.get(user_id, ''),
            created.date(), _sec(e.get('SECONDS')), _str(e.get('COMMENT_TEXT')), created,
        ))
    if task_time_rows:
        insert_client.insert(
            f"{db}.bitrix_task_time", task_time_rows,
            column_names=['ID', 'TASK_ID', 'TASK_TITLE', 'USER_ID', 'USER_NAME',
                          'WORK_DATE', 'SECONDS', 'COMMENT_TEXT', 'CREATED_DATE'],
        )
    log_func(f"INSERTED {len(task_time_rows)} task time records")
except Exception as exc:
    log_func(f"TASK TIME IMPORT FAILED: {exc}")

# 9. Отрезки работы по задачам из истории статусов («Начать» → «Пауза»/«Завершить»).
#    Отрезок «в работе» режется по сменам сотрудника из bitrix_workdays: каждой смене,
#    которую он пересекает, — своя строка (WORK_DATE = день смены). Время вне смен
#    не засчитывается. Если отрезок не пересекает ни одной смены — засчитываем
#    до NO_SHIFT_CAP_SEC от начала.
#    Пересчитываются задачи, изменённые с прошлого запуска, плюс задачи, чьи отрезки
#    ещё открыты или касались последних дней (их смены могли закрыться позже).
STATUS_IN_PROGRESS = '3'
NO_SHIFT_CAP_SEC = 10 * 3600
RECHECK_DAYS = 3
MSK = timezone(timedelta(hours=3))

def _msk(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s).replace(tzinfo=MSK) if s else None

def _load_shifts() -> dict[int, list[tuple]]:
    """{USER_ID: [(WORK_DATE, начало, конец | None), ...]} по возрастанию начала."""
    rows = ch_query.execute(
        f"SELECT USER_ID, WORK_DATE, toString(DATE_START), toString(DATE_FINISH) "
        f"FROM {db}.bitrix_workdays FINAL ORDER BY USER_ID, DATE_START"
    )
    shifts: dict[int, list[tuple]] = {}
    for uid, wd, st, fin in rows:
        shifts.setdefault(uid, []).append((wd, _msk(st), _msk(fin)))
    return shifts

def _split_by_shifts(start: datetime, end: datetime, shifts: list[tuple],
                     now: datetime) -> tuple[list[tuple], bool]:
    """[(WORK_DATE, начало куска, конец куска)], признак «подозрительный»."""
    pieces = []
    for wd, sh_start, sh_end in shifts:
        sh_end = sh_end or now                      # смена ещё открыта
        p_start, p_end = max(start, sh_start), min(end, sh_end)
        if p_end > p_start:
            pieces.append((wd, p_start, p_end))
    if pieces:
        return pieces, len(pieces) > 1              # тянулся через несколько смен
    cap_end = min(end, start + timedelta(seconds=NO_SHIFT_CAP_SEC))
    return [(start.date(), start, cap_end)], cap_end < end

def build_intervals(task: dict, history: list[dict], shifts: dict, user_names: dict,
                    now: datetime) -> list[tuple]:
    rows = []
    start, start_user = None, None

    def close(end: datetime | None, end_status: int):
        actual_end = end or now
        duration = max(int((actual_end - start).total_seconds()), 0)
        pieces, suspicious = _split_by_shifts(start, actual_end, shifts.get(start_user, []), now)
        for wd, p_start, p_end in pieces:
            rows.append((
                _int(task.get('id')), _str(task.get('title')),
                start_user, user_names.get(start_user, ''),
                wd, start, end, end_status, p_start, p_end,
                duration, int((p_end - p_start).total_seconds()), 1 if suspicious else 0,
            ))

    for h in history:
        at = _local_dt(h['at'])
        if at is None:
            continue
        if h['to'] == STATUS_IN_PROGRESS and start is None:
            start, start_user = at, _int(h['user_id']) or _int(task.get('responsibleId'))
        elif h['from'] == STATUS_IN_PROGRESS and start is not None:
            close(at, _int(h['to']))
            start = None
    if start is not None:
        close(None, 0)
    return rows

INTERVALS_COLUMNS = [
    'TASK_ID', 'TASK_TITLE', 'USER_ID', 'USER_NAME', 'WORK_DATE', 'DATE_START', 'DATE_END',
    'END_STATUS', 'SEGMENT_START', 'SEGMENT_END', 'DURATION_SEC', 'WORK_SEC', 'IS_SUSPICIOUS',
]

try:
    intervals_run_start = datetime.utcnow().isoformat()
    intervals_from = filter_dt(checkpoint, 'task_intervals')
    tasks_by_id = {_int(t.get('id')): t for t in api.fetch_tasks_changed(intervals_from, log_func)}
    if intervals_from:
        recheck_ids = [r[0] for r in ch_query.execute(
            f"SELECT DISTINCT TASK_ID FROM {db}.bitrix_task_work_intervals FINAL "
            f"WHERE DATE_END IS NULL OR DATE_END >= now() - INTERVAL {RECHECK_DAYS} DAY"
        ) if r[0] not in tasks_by_id]
        for t in api.fetch_tasks_by_ids(recheck_ids, log_func):
            tasks_by_id[_int(t.get('id'))] = t
    shifts = _load_shifts()
    user_names = {
        _int(u.get('ID')): ' '.join(p for p in (_str(u.get('NAME')).strip(), _str(u.get('LAST_NAME')).strip()) if p)
        for u in api.fetch_users()
    }
    now_msk = datetime.now(MSK)
    interval_rows = []
    for i, (task_id, task) in enumerate(tasks_by_id.items(), 1):
        history = api.fetch_task_status_history(task_id)
        interval_rows.extend(build_intervals(task, history, shifts, user_names, now_msk))
        if i % 100 == 0:
            log_func(f"task status history: {i}/{len(tasks_by_id)} tasks")
    if tasks_by_id:
        # пересчитанные задачи заменяем целиком: число кусков по дням могло измениться
        ch_query.execute(
            f"ALTER TABLE {db}.bitrix_task_work_intervals DELETE WHERE TASK_ID IN %(ids)s",
            {'ids': tuple(tasks_by_id)}, settings={'mutations_sync': 1},
        )
    if interval_rows:
        insert_client.insert(f"{db}.bitrix_task_work_intervals", interval_rows, column_names=INTERVALS_COLUMNS)
    save_checkpoint(['task_intervals'], intervals_run_start)
    log_func(f"INSERTED {len(interval_rows)} task work segments ({len(tasks_by_id)} tasks, from={intervals_from})")
except Exception as exc:
    log_func(f"TASK INTERVALS IMPORT FAILED: {exc}")

# Deduplicate ReplacingMergeTree tables
for table in ('bitrix_leads', 'bitrix_deals', 'bitrix_statuses', 'bitrix_deal_categories', 'bitrix_workdays', 'bitrix_task_time', 'bitrix_task_work_intervals'):
    try:
        ch_query.execute(
            f"OPTIMIZE TABLE {db}.{table} FINAL",
            settings={'max_execution_time': 1800, 'max_memory_usage': 2000000000},
        )
        log_func(f"OPTIMIZED {table}")
    except Exception as exc:
        log_func(f"SKIPPED OPTIMIZE FOR {table}: {exc}")
log_func("OPTIMIZE FINAL done")
