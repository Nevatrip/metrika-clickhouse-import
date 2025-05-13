#!/usr/bin/env python

import clickhouse_connect as cc
from clickhouse_connect.driver.tools import insert_file

client = cc.get_client(host='localhost', username='user', password='password', database='metrika')

insert_file(client, 'data', 'response.csv', 'TSVWithNames', ['ip', 'paramsKey1', 'paramsKey2', 'clientID'])

