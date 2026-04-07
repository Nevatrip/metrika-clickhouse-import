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
CREATE TABLE IF NOT EXISTS {db}.yogile_cards (
    snapshot_time  DateTime,
    sprint_number  UInt32,
    id             String,
    task_id        String,
    task_id_common String,
    column_id      String,
    completed      UInt8,
    done_time      Nullable(DateTime),
    project_name   String,
    sp_frontend    Float64,
    sp_backend     Float64,
    assignee_ids   Array(String),
    sprint_name    String
) ENGINE = MergeTree
ORDER BY (toDate(snapshot_time), id)
""")

client.execute(f"ALTER TABLE {db}.yogile_cards ADD COLUMN IF NOT EXISTS sprint_name String DEFAULT ''")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.yogile_users (
    id   String,
    name String
) ENGINE = ReplacingMergeTree
ORDER BY id
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.yogile_card_titles (
    id         String,
    title      String,
    created_at Nullable(DateTime),
    board_id   String
) ENGINE = ReplacingMergeTree
ORDER BY id
""")

client.execute(f"ALTER TABLE {db}.yogile_card_titles ADD COLUMN IF NOT EXISTS created_at Nullable(DateTime)")
client.execute(f"ALTER TABLE {db}.yogile_card_titles ADD COLUMN IF NOT EXISTS board_id String DEFAULT ''")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.yogile_columns (
    id       String,
    name     String,
    board_id String
) ENGINE = ReplacingMergeTree
ORDER BY id
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.yogile_boards (
    id         String,
    name       String,
    project_id String
) ENGINE = ReplacingMergeTree
ORDER BY id
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.yogile_sprint_names (
    sprint_number UInt32,
    sprint_name   String,
    sprint_begin  DateTime
) ENGINE = ReplacingMergeTree
ORDER BY sprint_number
""")

client.execute(f"ALTER TABLE {db}.yogile_sprint_names ADD COLUMN IF NOT EXISTS sprint_begin DateTime DEFAULT toDateTime(0)")

print("Tables created successfully")
