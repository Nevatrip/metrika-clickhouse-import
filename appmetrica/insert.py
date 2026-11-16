#!/usr/bin/env python3
"""Импорт сырых данных AppMetrica (Logs API) в ClickHouse.

Данные грузятся по одному дню. Каждый день перезаливается целиком:
выгрузка скачивается во временный файл, затем партиция дня удаляется
и заполняется заново — так повторные запуски не создают дублей.
События от приложений приходят с опозданием, поэтому последние
RELOAD_DAYS дней перезаливаются на каждом запуске.
"""

import os
import tempfile
from datetime import date, datetime, timedelta

import clickhouse_connect as cc
from clickhouse_connect.driver.tools import insert_file
from clickhouse_driver import Client

import helpers.env as env
from helpers.api import export_to_file
from helpers.env import env_value_or_error
from helpers.sources import SOURCES


def log(s: str) -> None:
    print(datetime.now().isoformat(), s, flush=True)


host = env_value_or_error(env.CLICKHOUSE_HOST)
user = env_value_or_error(env.CLICKHOUSE_USER)
password = env_value_or_error(env.CLICKHOUSE_PASSWORD)
ch = Client(host=host, user=user, password=password)
insert_client = cc.get_client(host=host, username=user, password=password)

db = env_value_or_error(env.CLICKHOUSE_DATABASE)
token = env_value_or_error(env.APPMETRICA_TOKEN)
app_id = env_value_or_error(env.APPMETRICA_APP_ID)
first_date = date.fromisoformat(env_value_or_error(env.FIRST_DATE))
day_count = int(env_value_or_error(env.DAY_COUNT))
reload_days = int(env_value_or_error(env.RELOAD_DAYS))
poll_seconds = int(env_value_or_error(env.POLL_SECONDS))
poll_timeout = int(env_value_or_error(env.POLL_TIMEOUT))

yesterday = date.today() - timedelta(days=1)


def days_to_load(source: str, date_field: str) -> list[date]:
    last = ch.execute(f"SELECT max(toDate({date_field})) FROM {db}.{source}")[0][0]
    start = first_date
    if last and last > date(1970, 1, 1):
        start = max(first_date, last - timedelta(days=reload_days))
    end = min(yesterday, start + timedelta(days=day_count - 1))
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def load_day(source: str, s: dict, day: date) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, f'{source}.csv')
        export_to_file(
            token, app_id, source, s['fields'],
            f'{day} 00:00:00', f'{day} 23:59:59',
            path, poll_seconds, poll_timeout, log,
        )
        ch.execute(f"ALTER TABLE {db}.{source} DROP PARTITION '{day}'")
        if os.path.getsize(path) > 0:
            insert_file(
                insert_client, f'{db}.{source}', path, fmt='CSVWithNames',
                settings={'input_format_csv_empty_as_default': 1, 'date_time_input_format': 'best_effort'},
            )
        rows = ch.execute(f"SELECT count() FROM {db}.{source} WHERE toDate({s['date_field']}) = '{day}'")[0][0]
        log(f'{source} {day}: {rows} rows')


for source in env_value_or_error(env.APPMETRICA_SOURCES).split():
    s = SOURCES[source]
    days = days_to_load(source, s['date_field'])
    log(f'{source}: loading {len(days)} days {days[0] if days else ""}..{days[-1] if days else ""}')
    for day in days:
        load_day(source, s, day)

log('done')
