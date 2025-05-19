#!/usr/bin/env python

import tempfile as tmp
import clickhouse_connect as cc
from clickhouse_connect.driver.tools import insert_file
import requests as rq
import time
import copy

import clickhouse_driver as cd

from helpers.env import env_value_or_error
import helpers.env as env
from helpers import chosen_hit_params as chp, chosen_visit_params as cvp, funcs, urls

def loggers(s: str):
    print(s)

insert_client = cc.get_client(host=env_value_or_error(env.CLICKHOUSE_HOST), username=env_value_or_error(env.CLICKHOUSE_USER), password=env_value_or_error(env.CLICKHOUSE_PASSWORD))
join_client = cd.Client(host=env_value_or_error(env.CLICKHOUSE_HOST), user=env_value_or_error(env.CLICKHOUSE_USER), password=env_value_or_error(env.CLICKHOUSE_PASSWORD))

with open(env_value_or_error(env.DATE_FILE)) as f:
    dates = f.readline().split(',')

headers = {
    'Authorization': f"OAuth {env_value_or_error(env.METRIKA_KEY)}"
}

attributions = env_value_or_error(env.ATTRIBUTIONS).split()
visit_key = tuple(env_value_or_error(env.TEMP_VISIT_KEY).split())
hit_key = tuple(env_value_or_error(env.TEMP_HIT_KEY).split())

main_db_name = env_value_or_error(env.MAIN_DATABASE)
main_visit_prefix = env_value_or_error(env.VISIT_TABLE_PREFIX)
main_hit_prefix = env_value_or_error(env.HIT_TABLE_PREFIX)

main_visit_prefix = f"{main_db_name}.{main_visit_prefix}"
main_hit_prefix = f"{main_db_name}.{main_hit_prefix}"

visit_table_names = list(funcs.get_table_names(main_visit_prefix, attributions))
hit_table_names = list(funcs.get_table_names(main_hit_prefix, attributions))

visit_key = (visit_key[0], visit_key[1], visit_key[2])
hit_key = (hit_key[0], hit_key[1], hit_key[2])

temp_db_name = env_value_or_error(env.TEMP_DATABASE)

orig_visit_params = []
orig_hit_params = []

for i1, i2 in funcs.divide_yandex_params(cvp.params, int(env_value_or_error(env.METRIKA_CHAR_LIMIT)), [visit_key]):
    params = cvp.params[i1:i2]
    orig_visit_params.append(params)

loggers('VISITS DIVIDED')

for i1, i2 in funcs.divide_yandex_params(chp.params, int(env_value_or_error(env.METRIKA_CHAR_LIMIT)), [hit_key]):
    params = cvp.params[i1:i2]
    orig_hit_params.append(params)

loggers('HITS DIVIDED')

visit_params = copy.deepcopy(orig_visit_params)
hit_params = copy.deepcopy(orig_hit_params)

for i in range(len(visit_params)):
    visit_params[i].append(visit_key)

for i in range(len(hit_params)):
    hit_params[i].append(hit_key)

orig_visit_params[0].append(visit_key)
orig_hit_params[0].append(hit_key)

counter_id = int(env_value_or_error(env.METRIKA_COUNTER))

for attr_num, attr in enumerate(attributions):
    loggers('ATTRIBUTION ' + attr)
    ids: list[int] = []

    for i, p in enumerate(visit_params):
        visit_req_params = {
            'date1': dates[0],
            'date2': dates[1],
            'source': 'visits',
            'fields': ','.join(funcs.metrika_fields(p)),
            'attribution': attr
        }

        resp = rq.post(urls.create(counter_id), params=visit_req_params, headers=headers)
        body = resp.json()['log_request']

        ids.append(body['request_id'])

    loggers('CREATED REQUESTS' + str(ids))

    ready = False
    while not ready:
        ready = True
        for i, id in enumerate(ids):
            time.sleep(3)

            resp = rq.get(urls.check(counter_id, id), headers=headers)
            status = resp.json()['log_request']['status']

            checked = funcs.check_request_status(status)
            if checked is None:
                raise Exception('Error processing logs request')

            ready = ready and checked

    loggers('ALL READY')

    table_prefix = env_value_or_error(env.TEMP_VISIT_TABLE_PREFIX)
    prefixes = [f"{temp_db_name}.{table_prefix}{str(i + 1)}" for i in range(len(ids))]

    for i, id in enumerate(ids):
        loggers(f"INSERTING REQUEST #{id}")

        resp = rq.get(urls.check(counter_id, id), headers=headers)
        body = resp.json()['log_request']

        parts = len(body['parts'])

        loggers('TABLE PREFIX = ' + prefixes[i])

        for part in range(parts):
            loggers('PART #' + str(part))
            with tmp.NamedTemporaryFile('w+b') as f:
                downloaded = rq.get(urls.download(counter_id, id, part), headers=headers)

                # По каким-то причинам в метрике есть и нормальные запятые
                # И экранированные, из-за этого парсер кликхауса ломается
                text = downloaded.content.replace('\\\''.encode(), '\''.encode()) 

                index = text.find('\n'.encode())

                f.write(text[index + 1:])
                f.flush()

                insert_file(insert_client, prefixes[i], f.name, 'TSV', funcs.table_fields(visit_params[i]))
                loggers(f"INSERTED PART {part} in table {prefixes[i]}")

        resp = rq.post(urls.clean(counter_id, id), headers=headers)
        if resp.status_code == 200:
            loggers(f"CLEANED REQUEST #{id}")
        else:
            loggers(f"ERROR CLEANING REQUEST #{id}, STATUS = {resp.status_code}")

    loggers(f"IMPORTING DATA IN TABLE {visit_table_names[attr_num]}")
    q = funcs.join_temp_tables(visit_table_names[attr_num], prefixes, orig_visit_params, visit_key[2])
    join_client.execute(q)

    for t in prefixes:
        join_client.execute(f"DELETE FROM {t} WHERE 1")
        loggers(f"CLEANED TEMPORARY TABLE {t}")

