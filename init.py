#!/usr/bin/env python

from clickhouse_driver import Client
from helpers.env import env_value_or_error
import helpers.env as env
import requests as rq
import kek

client = Client(host=env_value_or_error(env.CLICKHOUSE_HOST), user=env_value_or_error(env.CLICKHOUSE_USER), password=env_value_or_error(env.CLICKHOUSE_PASSWORD))

client.execute(f"DROP DATABASE IF EXISTS {env_value_or_error(env.MAIN_DATABASE)}")
client.execute(f"DROP DATABASE IF EXISTS {env_value_or_error(env.TEMP_DATABASE)}")

attributions: list[str] = env_value_or_error(env.ATTRIBUTIONS).split()
visit_key = tuple(env_value_or_error(env.TEMP_VISIT_KEY).split())
hit_key = tuple(env_value_or_error(env.TEMP_HIT_KEY).split())

# Разделить параметры
# Включить в каждый

# client.execute('CREATE TABLE data (`ip` String, `paramsKey1` Array(String), `paramsKey2` Array(String), `clientID` UInt64 ) ENGINE = MergeTree ORDER BY clientID')

print(client.execute('SHOW TABLES'))

with open('response.csv') as file:
    rows = [line for line in csv.reader(file, delimiter='\t')]

data = []

a = map(int, input().split(' '))

# client.execute('INSERT INTO data VALUES', data, types_check=True)
# print(client.execute('SELECT COUNT(*) FROM data GROUP BY clientID'))

# client.execute('CREATE TABLE mysql_data ENGINE = MySQL('mysql:3306', data, data, root, secret)')
rows = client.execute('SELECT * FROM data JOIN mysql_data ON data.clientID = mysql_data.ym_id')

if (not isinstance(rows, list)):
    print('ERROR')
    exit(0)

for row in rows:
    print(row)

