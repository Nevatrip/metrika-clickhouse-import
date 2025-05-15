#!/usr/bin/env python

from clickhouse_driver import Client
from helpers.env import env_value_or_error
import helpers.env as env
import helpers.chosen_visit_params as cvp
import helpers.chosen_hit_params as chp
import helpers.funcs as funcs

client = Client(host=env_value_or_error(env.CLICKHOUSE_HOST), user=env_value_or_error(env.CLICKHOUSE_USER), password=env_value_or_error(env.CLICKHOUSE_PASSWORD))

client.execute(f"DROP DATABASE IF EXISTS {env_value_or_error(env.MAIN_DATABASE)}")
client.execute(f"DROP DATABASE IF EXISTS {env_value_or_error(env.TEMP_DATABASE)}")

client.execute(f"CREATE DATABASE {env_value_or_error(env.MAIN_DATABASE)}")
client.execute(f"CREATE DATABASE {env_value_or_error(env.TEMP_DATABASE)}")

attributions = env_value_or_error(env.ATTRIBUTIONS).split()
visit_key = tuple(env_value_or_error(env.TEMP_VISIT_KEY).split())
hit_key = tuple(env_value_or_error(env.TEMP_HIT_KEY).split())

visit_key = (visit_key[0], visit_key[1], visit_key[2])
hit_key = (hit_key[0], hit_key[1], hit_key[2])

visit_params = []
hit_params = []

for i1, i2 in funcs.divide_yandex_params(cvp.params, int(env_value_or_error(env.METRIKA_CHAR_LIMIT)), [visit_key]):
    params = cvp.params[i1:i2]
    params.append(visit_key)
    visit_params.append(params)

for i1, i2 in funcs.divide_yandex_params(chp.params, int(env_value_or_error(env.METRIKA_CHAR_LIMIT)), [hit_key]):
    params = cvp.params[i1:i2]
    params.append(hit_key)
    hit_params.append(params)

temp_visit_prefix = f"{env_value_or_error(env.TEMP_DATABASE)}.{env_value_or_error(env.TEMP_VISIT_TABLE_PREFIX)}"
for i, p in enumerate(visit_params):
    for q in funcs.create_table_queries(temp_visit_prefix + str(i + 1), p, [visit_key[2]]):
        client.execute(q)

temp_hit_prefix = f"{env_value_or_error(env.TEMP_DATABASE)}.{env_value_or_error(env.TEMP_HIT_TABLE_PREFIX)}"
for i, p in enumerate(hit_params):
    for q in funcs.create_table_queries(temp_hit_prefix + str(i + 1), p, [hit_key[2]]):
        client.execute(q)

all_visit_params = cvp.params
all_hit_params = chp.params

all_visit_params.append(visit_key)
all_hit_params.append(hit_key)

main_visit_prefix = f"{env_value_or_error(env.MAIN_DATABASE)}.{env_value_or_error(env.VISIT_TABLE_PREFIX)}"
main_hit_prefix = f"{env_value_or_error(env.MAIN_DATABASE)}.{env_value_or_error(env.HIT_TABLE_PREFIX)}"

main_visit_keys = env_value_or_error(env.MAIN_VISIT_PRIMARY_KEYS).split()
main_hit_keys = env_value_or_error(env.MAIN_HIT_PRIMATY_KEYS).split()

for q in funcs.create_table_queries(main_visit_prefix, all_visit_params, main_visit_keys, attributions):
    client.execute(q)

for q in funcs.create_table_queries(main_hit_prefix, all_hit_params, main_hit_keys, attributions):
    client.execute(q)

with open(env_value_or_error(env.DATE_FILE), 'w') as f:
    f.write(funcs.get_init_dates(env_value_or_error(env.FIRST_DATE), 97))

