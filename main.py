#!/usr/bin/env python

from re import split
from clickhouse_driver import Client
import csv
import helpers.env as env

# docker exec -i abb2236ee2aa clickhouse-client --query 'INSERT INTO metrika.data SELECT ip, paramsKey1, paramsKey2, clientID FROM input('ip String, paramsKey1 Array(String), paramsKey2 Array(String), clientID UInt64') FORMAT TabSeparated' < reponse1.tsv

key = env.values['METRIKA_KEY']

params = ['ym:s:ipAddress', 'ym:s:parsedParamsKey1', 'ym:s:parsedParamsKey2', 'ym:s:clientID']

counter_id = env.values['METRIKA_COUNTER']

client = Client(host=env.values['CLICKHOUSE_HOST'], user=env.values['CLICKHOUSE_USER'], password=env.values['CLICKHOUSE_PASSWORD'], database=env.values['CLICKHOUSE_DATABASE'])

# client.execute('DROP TABLE data')
# client.execute('CREATE TABLE data (`ip` String, `paramsKey1` Array(String), `paramsKey2` Array(String), `clientID` UInt64 ) ENGINE = MergeTree ORDER BY clientID')

print(client.execute('SHOW TABLES'))

with open('response.csv') as file:
    rows = [line for line in csv.reader(file, delimiter='\t')]

data = []

a = map(int, input().split(' '))

def process_array(item: str):
    if item[0] != '[':
        raise Exception('NOT ARRAY')

    return [s.strip(' \'\'') for s in split(',', item.strip(' []')) if s.strip(' \'\'') != '']

def process_int(item: str):
    if not item.isdigit():
        raise Exception('NOT INTEGER')

    return int(item)

# for row in rows[1:]:
#     data_row = []
#
#     data.append([row[0], process_array(row[1]), process_array(row[2]), process_int(row[3])])
#
#
# for i in data:
#     print(i)

# client.execute('INSERT INTO data VALUES', data, types_check=True)
# print(client.execute('SELECT COUNT(*) FROM data GROUP BY clientID'))

# client.execute('CREATE TABLE mysql_data ENGINE = MySQL('mysql:3306', data, data, root, secret)')
rows = client.execute('SELECT * FROM data JOIN mysql_data ON data.clientID = mysql_data.ym_id')

if (not isinstance(rows, list)):
    print('ERROR')
    exit(0)

for row in rows:
    print(row)

