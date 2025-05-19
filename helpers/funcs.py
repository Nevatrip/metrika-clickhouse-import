from collections.abc import Callable
import datetime as dt
import time
import requests as rq
import tempfile as tmp
import clickhouse_connect.driver.client as cc
from clickhouse_connect.driver.tools import insert_file
import clickhouse_driver as cd

import helpers.urls as urls

def get_table_names(prefix: str, attributions: list[str]|None = None):
    """Получить генератор, выдающий имена таблиц clickhouse по атрибуциям"""
    if attributions is None:
        yield prefix
    else:
        for a in attributions:
            yield f"{prefix}_{a}"

def create_table_queries(prefix: str, params: list[tuple[str, str, str]], tree_keys: list[str], attributions: list[str]|None = None):
    """Получить генератор, выдающий запросы на создание таблиц clickhouse по агрегациям"""
    attr_list = [f"`{p[2]}` {p[1]}" for p in params]

    order = 'tuple()'
    if len(tree_keys) == 1:
        order = tree_keys[0]
    elif len(tree_keys) > 1:
        order = f"({', '.join(tree_keys)})"

    for table_name in get_table_names(prefix, attributions):
        q = f"CREATE TABLE {table_name} ("
        q += ", ".join(attr_list)
        q += f") ENGINE = MergeTree ORDER BY {order}"

        yield q

def table_fields(params: list[tuple[str, str, str]]):
    """Получить список имён полей таблицы clickhouse"""
    return [p[2] for p in params]

def metrika_fields(params: list[tuple[str, str, str]]):
    """Получить список имён полей таблицы clickhouse"""
    return [p[0] for p in params]

def divide_yandex_params(params: list[tuple[str, str, str]], limit: int = 3000, added_params: list[tuple[str, str, str]] = []):
    """Получить генератор, выдающий индексы для разделения параметров запроса к метрике с учётом дополнительных параметров, включённых во все запросы"""
    for p in added_params:
        limit -= len(p[0])

    if limit <= 0:
        return None

    char_count = 0
    first_index = 0
    last_index = 0

    for p in params:
        char_count += len(p[0]) + 1 # Ещё один для запятой

        if char_count <= limit:
            last_index += 1
        else:
            if first_index == last_index:
                raise Exception('Too long parameter')

            yield (first_index, last_index)

            first_index = last_index
            last_index += 1
            char_count = len(p[0])

    if first_index == last_index:
        raise Exception('Too long parameter')
    yield (first_index, last_index)

def get_init_dates(date: str, period_days: int):
    """Получить даты, выставляемые при инициализации"""
    DAY_SECONDS = 24 * 3600

    first_date = dt.datetime.fromisoformat(date)

    yesterday = time.time() - DAY_SECONDS

    period_delta = dt.timedelta(days=period_days)
    after_period = (first_date + period_delta).timestamp()

    last_date = dt.date.fromtimestamp(min(yesterday, after_period))

    return f"{first_date.date().isoformat()},{last_date.isoformat()}"

def get_next_dates(date: str, period_days: int):
    """Получить даты, выставляемые после вставки метрики в таблицы"""
    day_delta = dt.timedelta(days=1)

    first_date = dt.datetime.fromisoformat(date) + day_delta

    today = time.time()

    period_delta = dt.timedelta(days=period_days)
    after_period = (first_date + period_delta).timestamp()

    last_date = dt.date.fromtimestamp(min(today, after_period))

    return f"{first_date.date().isoformat()},{last_date.isoformat()}"

def check_request_status(status: str):
    """Проверить статус запроса
    Вернёт `None` при ошибке"""
    success = ['processed']
    wait = ['created']

    if status in success:
        return True
    elif status in wait:
        return False
    
    return None

def join_temp_tables(main_table_name: str, table_names: list[str], tables_fields: list[list[tuple[str, str, str]]], primary_key: str):
    """Получить запрос на объединение временных таблиц и вставку данных в постоянную таблицу"""
    q = f"INSERT INTO TABLE {main_table_name} "

    statements: list[str] = []

    for l in tables_fields:
        for p in table_fields(l):
            statements.append(p)

    q += '(' + ', '.join(statements) + ')'
    q += " SELECT "

    statements: list[str] = []

    for table, fields in zip(table_names, tables_fields):
        for field in fields:
            statements.append(f"{table}.{field[2]} AS {field[2]}")

    q += ', '.join(statements)
    q += f" FROM {table_names[0]} "

    for i in range(1, len(table_names)):
        q += f"JOIN {table_names[i]} ON {table_names[i - 1]}.{primary_key} = {table_names[i]}.{primary_key} "

    return q

def insert_data(
    attributions: list[str],
    source: str, # hits или visits
    params: list[list[tuple[str, str, str]]],
    date1: str,
    date2: str,
    counter_id: str|int,
    request_headers: dict[str, str], # Обязательно должен быть токен OAuth
    temp_table_prefix: str,
    main_table_prefix: str,
    insert_client: cc.Client,
    join_client: cd.Client,
    orig_params: list[list[tuple[str, str, str]]],
    temp_primary_key: str,
    log_func: Callable[[str], None],
):
    """Основная функция выгрузки данных"""
    main_table_names = list(get_table_names(main_table_prefix, attributions))

    for attr_num, attr in enumerate(attributions):
        log_func('ATTRIBUTION ' + attr)
        ids: list[int] = []

        for p in params:
            request_params = {
                'date1': date1,
                'date2': date2,
                'source': source,
                'fields': ','.join(metrika_fields(p)),
                'attribution': attr
            }

            resp = rq.post(urls.create(counter_id), params=request_params, headers=request_headers)
            body = resp.json()['log_request']

            ids.append(body['request_id'])

        log_func('CREATED REQUESTS' + str(ids))

        ready = False
        while not ready:
            ready = True
            for i, id in enumerate(ids):
                time.sleep(3)

                resp = rq.get(urls.check(counter_id, id), headers=request_headers)
                status = resp.json()['log_request']['status']

                checked = check_request_status(status)
                if checked is None:
                    raise Exception('Error processing logs request')

                ready = ready and checked

        log_func('ALL READY')

        prefixes = [f"{temp_table_prefix}{str(i + 1)}" for i in range(len(ids))]

        for i, id in enumerate(ids):
            log_func(f"INSERTING REQUEST #{id}")

            resp = rq.get(urls.check(counter_id, id), headers=request_headers)
            body = resp.json()['log_request']

            parts = len(body['parts'])

            log_func('TABLE PREFIX = ' + prefixes[i])

            for part in range(parts):
                log_func('PART #' + str(part))
                with tmp.NamedTemporaryFile('w+b') as f:
                    downloaded = rq.get(urls.download(counter_id, id, part), headers=request_headers)

                    # По каким-то причинам в метрике есть и нормальные запятые
                    # И экранированные, из-за этого парсер кликхауса ломается
                    text = downloaded.content.replace('\\\''.encode(), '\''.encode()) 

                    index = text.find('\n'.encode())

                    f.write(text[index + 1:])
                    f.flush()

                    insert_file(insert_client, prefixes[i], f.name, 'TSV', table_fields(params[i]))
                    log_func(f"INSERTED PART {part} in table {prefixes[i]}")

            resp = rq.post(urls.clean(counter_id, id), headers=request_headers)
            if resp.status_code == 200:
                log_func(f"CLEANED REQUEST #{id}")
            else:
                log_func(f"ERROR CLEANING REQUEST #{id}, STATUS = {resp.status_code}")

        log_func(f"IMPORTING DATA IN TABLE {main_table_names[attr_num]}")
        q = join_temp_tables(main_table_names[attr_num], prefixes, orig_params, temp_primary_key)
        join_client.execute(q)

        for t in prefixes:
            join_client.execute(f"DELETE FROM {t} WHERE 1")
            log_func(f"CLEANED TEMPORARY TABLE {t}")

