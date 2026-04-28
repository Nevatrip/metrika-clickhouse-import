from collections.abc import Callable
import datetime as dt
import heapq
import itertools
import os
import subprocess
import time
import requests as rq
import tempfile as tmp
import clickhouse_connect.driver.client as cc
from clickhouse_connect.driver.tools import insert_file
import clickhouse_driver as cd
import re

import helpers.urls as urls

def get_table_names(prefix: str, attributions: list[str]|None = None, part_number: int|None = None):
    """Получить генератор, выдающий имена таблиц clickhouse по атрибуциям"""
    if attributions is None:
        yield get_table_name(prefix, None, part_number)
    else:
        for a in attributions:
            yield get_table_name(prefix, a, part_number)

def get_table_name(prefix: str, attribution: str|None = None, number: int|None = None):
    arr = [prefix]
    if attribution is not None:
        arr.append(attribution)
    if number is not None:
        arr.append(str(number))

    return '_'.join(arr)

def create_table_queries(prefix: str, params: list[tuple[str, str, str]], tree_keys: list[str], attributions: list[str]|None = None, part_number: int|None = None):
    """Получить генератор, выдающий запросы на создание таблиц clickhouse по агрегациям"""
    attr_list = [f"`{p[2]}` {p[1]}" for p in params]

    order = 'tuple()'
    if len(tree_keys) == 1:
        order = tree_keys[0]
    elif len(tree_keys) > 1:
        order = f"({', '.join(tree_keys)})"

    for table_name in get_table_names(prefix, attributions, part_number):
        q = f"CREATE TABLE {table_name} ("
        q += ", ".join(attr_list)
        q += f") ENGINE = ReplacingMergeTree ORDER BY {order} SETTINGS min_bytes_for_wide_part = 10000000000, min_rows_for_wide_part = 1000000000"

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

    DAY_SECONDS = 24 * 3600
    yesterday = time.time() - DAY_SECONDS

    period_delta = dt.timedelta(days=period_days)
    after_period = (first_date + period_delta).timestamp()

    last_date = dt.date.fromtimestamp(min(yesterday, after_period))

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

def join_temp_tables(main_table_name: str, table_names: list[str], tables_fields: list[list[tuple[str, str, str]]], primary_key: str, bucket: int | None = None, num_buckets: int | None = None):
    """Получить запрос объединения временных таблиц с опциональной бакетизацией по ключу.
    При указании bucket/num_buckets обе стороны JOIN фильтруются через cityHash64(pk) % N = bucket,
    что ограничивает память JOIN."""
    col_list: list[str] = []
    for l in tables_fields:
        for p in table_fields(l):
            col_list.append(p)

    aliases = [f"t{i}" for i in range(len(table_names))]

    select_parts: list[str] = []
    for alias, fields in zip(aliases, tables_fields):
        for field in fields:
            select_parts.append(f"{alias}.{field[2]} AS {field[2]}")

    where = ""
    if bucket is not None and num_buckets is not None:
        where = f" WHERE cityHash64({primary_key}) % {num_buckets} = {bucket}"

    from_clause = f"(SELECT * FROM {table_names[0]}{where}) AS {aliases[0]}"
    for i in range(1, len(table_names)):
        from_clause += f" JOIN (SELECT * FROM {table_names[i]}{where}) AS {aliases[i]}"
        from_clause += f" ON {aliases[i-1]}.{primary_key} = {aliases[i]}.{primary_key}"

    return f"INSERT INTO TABLE {main_table_name} ({', '.join(col_list)}) SELECT {', '.join(select_parts)} FROM {from_clause}"

def transform_enum(text: bytes):
    enum = [
        'view_item_list',
        'click',
        'detail',
        'add',
        'purchase',
        'remove',
    ]

    for s in enum:
        pattern = f"(\\[|,){s}".encode()
        repl = f"\\g<1>'{s}'".encode()
        text = re.sub(pattern, repl, text)

    return text

def insert_data(
    attributions: list[str],
    source: str, # hits или visits
    params: list[list[tuple[str, str, str]]],
    date1: str,
    date2: str,
    counter_id: str|int,
    request_headers: dict[str, str], # Обязательно должен быть токен OAuth
    main_table_prefix: str,
    insert_client: cc.Client,
    join_client: cd.Client,
    orig_params: list[list[tuple[str, str, str]]],
    temp_primary_key: str,
    log_func: Callable[[str], None],
):
    """Основная функция выгрузки данных.
    Я (а точнее братишка клод) сделал обработку данных в скрипте,
    потому что кликхаусу не хватало оперативной памяти для объединения всех частей запроса от метрики"""
    main_table_names = list(get_table_names(main_table_prefix, attributions))

    # Гарантируем compact-формат для основных таблиц
    # for t in main_table_names:
    #     join_client.execute(
    #         f"ALTER TABLE {t} MODIFY SETTING "
    #         f"min_bytes_for_wide_part = 10000000000, "
    #         f"min_rows_for_wide_part = 1000000000"
    #     )

    # Строим упорядоченный список всех колонок основной таблицы
    all_cols: list[str] = []
    seen_cols: set[str] = set()
    for param_set in orig_params:
        for p in param_set:
            col = p[2]
            if col not in seen_cols:
                all_cols.append(col)
                seen_cols.add(col)

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
            body = resp.json()

            if 'log_request' not in body:
                raise Exception('Error creating logs request: ' + str(body))

            ids.append(body['log_request']['request_id'])

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

        sorted_tmp_files: list[tuple[str, list[str], int]] = []  # (path, ch_cols, pk_pos)
        tmp_paths: set[str] = set()  # все временные файлы, требующие очистки
        output_f = None

        try:
            # Скачиваем каждый param split в отдельный файл и сортируем по pk
            for i, id_val in enumerate(ids):
                ch_cols = table_fields(params[i])

                if temp_primary_key not in ch_cols:
                    raise Exception(f"Primary key {temp_primary_key} not found in params[{i}]")
                pk_pos = ch_cols.index(temp_primary_key)

                raw_f = tmp.NamedTemporaryFile('wb', suffix='.tsv', delete=False)
                raw_path = raw_f.name
                tmp_paths.add(raw_path)
                try:
                    resp = rq.get(urls.check(counter_id, id_val), headers=request_headers)
                    parts_count = len(resp.json()['log_request']['parts'])

                    for part in range(parts_count):
                        log_func(f"DOWNLOADING REQUEST #{id_val} PART #{part}")
                        downloaded = rq.get(urls.download(counter_id, id_val, part), headers=request_headers)

                        # По каким-то причинам в метрике есть и нормальные запятые
                        # И экранированные, из-за этого парсер кликхауса ломается
                        text = downloaded.content.replace(b"\\'", b"'")

                        # В колонке eventsProductType приходит массив строк без кавычек
                        # Приходится регулярными выражениями добалять им кавычки
                        text = transform_enum(text)

                        nl_pos = text.find(b'\n')
                        if nl_pos == -1:
                            continue

                        for line_bytes in text[nl_pos + 1:].split(b'\n'):
                            if not line_bytes.strip():
                                continue
                            values = line_bytes.decode('utf-8').split('\t')
                            if len(values) != len(ch_cols):
                                continue
                            raw_f.write(line_bytes + b'\n')

                    raw_f.flush()
                finally:
                    raw_f.close()

                sorted_path = raw_path + '.sorted'
                tmp_paths.add(sorted_path)
                subprocess.run(
                    ['sort', '-t', '\t', f'-k{pk_pos + 1},{pk_pos + 1}', '-o', sorted_path, raw_path],
                    check=True
                )
                os.unlink(raw_path)
                tmp_paths.discard(raw_path)
                sorted_tmp_files.append((sorted_path, ch_cols, pk_pos))

                resp = rq.post(urls.clean(counter_id, id_val), headers=request_headers)
                if resp.status_code == 200:
                    log_func(f"CLEANED REQUEST #{id_val}")
                else:
                    log_func(f"ERROR CLEANING REQUEST #{id_val}, STATUS = {resp.status_code}")

            # Merge-sort отсортированных файлов по pk — в памяти одновременно по 1 строке из каждого файла
            def iter_sorted_tsv(path: str):
                with open(path, 'r', encoding='utf-8') as fh:
                    for line in fh:
                        line = line.rstrip('\n')
                        if line:
                            yield line.split('\t')

            col_lists = [ch_cols for _, ch_cols, _ in sorted_tmp_files]
            pk_positions = [pk_pos for _, _, pk_pos in sorted_tmp_files]
            seq = itertools.count()

            def make_tagged(it, idx):
                for row in it:
                    yield (row[pk_positions[idx]], idx, next(seq), row)

            file_iters = [iter_sorted_tsv(path) for path, _, _ in sorted_tmp_files]
            merged_iter = heapq.merge(*[make_tagged(it, i) for i, it in enumerate(file_iters)])

            output_f = tmp.NamedTemporaryFile('w', suffix='.merged.tsv', delete=False, encoding='utf-8')
            tmp_paths.add(output_f.name)

            rows_count = 0
            current_pk: str | None = None
            current_row: dict[str, str] = {}

            for pk_val, file_idx, _, values in merged_iter:
                if pk_val != current_pk:
                    if current_pk is not None:
                        output_f.write('\t'.join(current_row.get(col, '') for col in all_cols) + '\n')
                        rows_count += 1
                    current_pk = pk_val
                    current_row = {}
                for col, val in zip(col_lists[file_idx], values):
                    current_row[col] = val

            if current_pk is not None:
                output_f.write('\t'.join(current_row.get(col, '') for col in all_cols) + '\n')
                rows_count += 1

            output_f.flush()
            output_f.close()

            log_func(f"MERGED {rows_count} ROWS ON DISK, IMPORTING INTO {main_table_names[attr_num]}")

            insert_settings = {
                'async_insert': 0,
                'max_insert_block_size': 10000,
                'max_compress_block_size': 65536,
            }

            join_client.execute("SYSTEM STOP MERGES")
            for _ in range(60):
                result = join_client.execute("SELECT count() FROM system.merges")
                if result[0][0] == 0:
                    break
                time.sleep(2)

            try:
                insert_file(insert_client, main_table_names[attr_num], output_f.name, 'TSV', all_cols, settings=insert_settings)
            finally:
                join_client.execute("SYSTEM START MERGES")

            log_func(f"IMPORTED {rows_count} ROWS INTO {main_table_names[attr_num]}")
            join_client.execute(f"OPTIMIZE TABLE {main_table_names[attr_num]} FINAL", settings={'max_execution_time': 1800})
            log_func(f"OPTIMIZED {main_table_names[attr_num]}")

        finally:
            if output_f is not None and not output_f.closed:
                output_f.close()
            for path in tmp_paths:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass

