#!/usr/bin/env python3

from clickhouse_driver import Client

import helpers.env as env
from helpers.env import env_value_or_error

client = Client(
    host=env_value_or_error(env.CLICKHOUSE_HOST),
    user=env_value_or_error(env.CLICKHOUSE_USER),
    password=env_value_or_error(env.CLICKHOUSE_PASSWORD),
)

db = env_value_or_error(env.CLICKHOUSE_DATABASE)

client.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_leads (
    ID              UInt64,
    TITLE           String,
    STATUS_ID       String,
    SOURCE_ID       String,
    DATE_CREATE     DateTime,
    DATE_MODIFY     DateTime,
    ASSIGNED_BY_ID  UInt32,
    UTM_SOURCE      String,
    UTM_MEDIUM      String,
    UTM_CAMPAIGN    String,
    UTM_CONTENT     String,
    UTM_TERM        String,
    PHONE           String,
    EMAIL           String,
    OPPORTUNITY     Float64,
    CURRENCY_ID     String,
    raw_data        String
) ENGINE = ReplacingMergeTree
ORDER BY ID
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_deals (
    ID              UInt64,
    TITLE           String,
    STAGE_ID        String,
    TYPE_ID         String,
    SOURCE_ID       String,
    DATE_CREATE     DateTime,
    DATE_MODIFY     DateTime,
    ASSIGNED_BY_ID  UInt32,
    CONTACT_ID      UInt64,
    LEAD_ID         UInt64,
    CATEGORY_ID     UInt32,
    OPPORTUNITY     Float64,
    CURRENCY_ID     String,
    UTM_SOURCE      String,
    UTM_MEDIUM      String,
    UTM_CAMPAIGN    String,
    UTM_CONTENT     String,
    UTM_TERM        String,
    raw_data        String
) ENGINE = ReplacingMergeTree
ORDER BY ID
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_status_history (
    ID              UInt64,
    ENTITY_TYPE_ID  UInt8,
    ENTITY_ID       UInt64,
    STATUS_FROM_ID  String,
    STATUS_TO_ID    String,
    CREATED_DATE    DateTime,
    USER_ID         UInt32
) ENGINE = MergeTree
ORDER BY (ENTITY_TYPE_ID, ENTITY_ID, CREATED_DATE)
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_statuses (
    ENTITY_ID   String,
    STATUS_ID   String,
    NAME        String,
    SORT        UInt32,
    COLOR       String
) ENGINE = ReplacingMergeTree
ORDER BY (ENTITY_ID, STATUS_ID)
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_fields (
    entity_type  String,
    field_id     String,
    title        String,
    field_type   String,
    is_multiple  UInt8,
    is_required  UInt8
) ENGINE = ReplacingMergeTree
ORDER BY (entity_type, field_id)
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_deal_categories (
    ID    UInt32,
    NAME  String,
    SORT  UInt32
) ENGINE = ReplacingMergeTree
ORDER BY ID
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_enum_values (
    entity_type  String,
    field_id     String,
    item_id      UInt32,
    item_value   String
) ENGINE = ReplacingMergeTree
ORDER BY (entity_type, field_id, item_id)
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.bitrix_checkpoint (
    entity     String,
    synced_at  DateTime
) ENGINE = ReplacingMergeTree
ORDER BY entity
""")

print("Tables created successfully")
