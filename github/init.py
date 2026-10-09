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

# Только first-parent цепочка ветки (main) — то, что реально попало в ветку.
client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.commits (
    repo           LowCardinality(String),
    sha            String,
    committed_at   DateTime,
    authored_at    DateTime,
    author_login   String,
    author_name    String,
    subject        String,
    message        String,
    prefix         LowCardinality(String),
    scope          String,
    task_id        String,
    parents_count  UInt8,
    additions      UInt32,
    deletions      UInt32,
    files_count    UInt32,
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (repo, sha)
""")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.commit_files (
    repo       LowCardinality(String),
    sha        String,
    filename   String,
    status     LowCardinality(String),
    additions  UInt32,
    deletions  UInt32
) ENGINE = ReplacingMergeTree
ORDER BY (repo, sha, filename)
""")

print("Tables created successfully")
