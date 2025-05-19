import datetime as dt
import time

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
    DAY_SECONDS = 24 * 3600

    first_date = dt.datetime.fromisoformat(date)

    yesterday = time.time() - DAY_SECONDS

    period_delta = dt.timedelta(days=period_days)
    after_period = (first_date + period_delta).timestamp()

    last_date = dt.date.fromtimestamp(min(yesterday, after_period))

    return f"{first_date.date().isoformat()},{last_date.isoformat()}"

def get_next_dates(date: str, period_days: int):
    day_delta = dt.timedelta(days=1)

    first_date = dt.datetime.fromisoformat(date) + day_delta

    today = time.time()

    period_delta = dt.timedelta(days=period_days)
    after_period = (first_date + period_delta).timestamp()

    last_date = dt.date.fromtimestamp(min(today, after_period))

    return f"{first_date.date().isoformat()},{last_date.isoformat()}"

def check_request_status(status: str):
    success = ['processed']
    wait = ['created']

    if status in success:
        return True
    elif status in wait:
        return False
    
    return None

def join_temp_tables(main_table_name: str, table_names: list[str], tables_fields: list[list[tuple[str, str, str]]], primary_key: str):
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

