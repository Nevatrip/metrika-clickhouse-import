#!/usr/bin/env python3

from clickhouse_driver import Client

import helpers.env as env
from helpers.env import env_value_or_error
from helpers.sources import SOURCES, column_type

client = Client(
    host=env_value_or_error(env.CLICKHOUSE_HOST),
    user=env_value_or_error(env.CLICKHOUSE_USER),
    password=env_value_or_error(env.CLICKHOUSE_PASSWORD),
)

db = env_value_or_error(env.CLICKHOUSE_DATABASE)
client.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

for source in env_value_or_error(env.APPMETRICA_SOURCES).split():
    s = SOURCES[source]
    columns = ',\n    '.join(f"`{f}` {column_type(f)}" for f in s['fields'])
    client.execute(f"""
CREATE TABLE IF NOT EXISTS {db}.{source} (
    {columns}
) ENGINE = MergeTree
PARTITION BY toDate({s['date_field']})
ORDER BY ({', '.join(s['order_by'])})
""")
    # Новые поля, добавленные в sources.py после создания таблицы
    for f in s['fields']:
        client.execute(f"ALTER TABLE {db}.{source} ADD COLUMN IF NOT EXISTS `{f}` {column_type(f)}")

print('Tables created successfully')
