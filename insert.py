#!/usr/bin/env python

import clickhouse_driver as cd
import clickhouse_connect as cc
import copy
import os
from datetime import datetime as dt

from helpers.env import env_value_or_error
import helpers.env as env
from helpers import chosen_hit_params as chp, chosen_visit_params as cvp, funcs

def loggers(s: str):
    print(dt.now().isoformat(), s)

def no_loggers(s: str):
    pass

log_func = no_loggers
if env_value_or_error(env.LOG_ENABLE) == 'true':
    log_func = loggers

insert_client = cc.get_client(host=env_value_or_error(env.CLICKHOUSE_HOST), username=env_value_or_error(env.CLICKHOUSE_USER), password=env_value_or_error(env.CLICKHOUSE_PASSWORD))
join_client = cd.Client(host=env_value_or_error(env.CLICKHOUSE_HOST), user=env_value_or_error(env.CLICKHOUSE_USER), password=env_value_or_error(env.CLICKHOUSE_PASSWORD))

datefile = os.path.dirname(os.path.realpath(__file__)) + '/' + env_value_or_error(env.DATE_FILE)

with open(datefile) as f:
    dates = f.readline().strip().split(',')

if len(dates) != 2:
    raise Exception('INVALID DATES')

log_func(f"STARTING TO IMPORT DATES FROM {dates[0]} TO {dates[1]}")

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

visit_key = (visit_key[0], visit_key[1], visit_key[2])
hit_key = (hit_key[0], hit_key[1], hit_key[2])

temp_db_name = env_value_or_error(env.TEMP_DATABASE)

temp_visit_prefix = env_value_or_error(env.TEMP_VISIT_TABLE_PREFIX)
temp_hit_prefix = env_value_or_error(env.TEMP_HIT_TABLE_PREFIX)

temp_visit_prefix = f"{temp_db_name}.{temp_visit_prefix}"
temp_hit_prefix = f"{temp_db_name}.{temp_hit_prefix}"

orig_visit_params = []
orig_hit_params = []

for i1, i2 in funcs.divide_yandex_params(cvp.params, int(env_value_or_error(env.METRIKA_CHAR_LIMIT)), [visit_key]):
    params = cvp.params[i1:i2]
    orig_visit_params.append(params)

log_func('VISITS DIVIDED')

for i1, i2 in funcs.divide_yandex_params(chp.params, int(env_value_or_error(env.METRIKA_CHAR_LIMIT)), [hit_key]):
    params = chp.params[i1:i2]
    orig_hit_params.append(params)

log_func('HITS DIVIDED')

visit_params = copy.deepcopy(orig_visit_params)
hit_params = copy.deepcopy(orig_hit_params)

for i in range(len(visit_params)):
    visit_params[i].append(visit_key)

for i in range(len(hit_params)):
    hit_params[i].append(hit_key)

orig_visit_params[0].append(visit_key)
orig_hit_params[0].append(hit_key)

counter_id = int(env_value_or_error(env.METRIKA_COUNTER))

log_func('STARTING TO IMPORT VISITS')
funcs.insert_data(
    attributions,
    'visits',
    visit_params,
    dates[0],
    dates[1],
    counter_id,
    headers,
    temp_visit_prefix,
    main_visit_prefix,
    insert_client,
    join_client,
    orig_visit_params,
    visit_key[2],
    log_func,
)

log_func('STARTING TO IMPORT HITS')
funcs.insert_data(
    attributions,
    'hits',
    hit_params,
    dates[0],
    dates[1],
    counter_id,
    headers,
    temp_hit_prefix,
    main_hit_prefix,
    insert_client,
    join_client,
    orig_hit_params,
    hit_key[2],
    log_func
)

day_count = int(env_value_or_error(env.DAY_COUNT))

with open(datefile, 'w') as f:
    f.write(funcs.get_next_dates(dates[1], day_count))

