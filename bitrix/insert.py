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

def transform_status(r: dict) -> tuple:
    return (
        _str(r.get('ENTITY_ID')),
        _str(r.get('STATUS_ID')),
        _str(r.get('NAME')),
        _int(r.get('SORT')),
        _str(r.get('COLOR')),
    )

STATUSES_COLUMNS = ['ENTITY_ID', 'STATUS_ID', 'NAME', 'SORT', 'COLOR']

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
        [('DEAL_STAGE', _str(s.get('STATUS_ID')), _str(s.get('NAME')), _int(s.get('SORT')), _str(s.get('COLOR'))) for s in stages_from_categories],
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

# Deduplicate ReplacingMergeTree tables
for table in ('bitrix_leads', 'bitrix_deals', 'bitrix_statuses', 'bitrix_deal_categories'):
    ch_query.execute(f"OPTIMIZE TABLE {db}.{table} FINAL")
log_func("OPTIMIZE FINAL done")
