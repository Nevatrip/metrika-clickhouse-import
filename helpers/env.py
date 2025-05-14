from dotenv import dotenv_values

__values: dict[str, str | None] = {
    'CLICKHOUSE_HOST': 'localhost',
    'CLICKHOUSE_USER': 'root',
    'CLICKHOUSE_PASSWORD': 'secret',

    'MAIN_DATABASE': 'metrika',
    'TEMP_DATABASE': 'temp_metrika',

    'METRIKA_COUNTER': None,
    'METRIKA_KEY': None,

    'METRIKA_CHAR_LIMIT': '3000',

    'TEMP_HIT_TABLE_PREFIX': 'temp_hits',
    'HIT_TABLE_PREFIX': 'hits',

    'TEMP_VISIT_TABLE_PREFIX': 'temp_visits',
    'VISIT_TABLE_PREFIX': 'visits',

    'TEMP_VISIT_KEY': 'ym:s:visitID UInt64 visitID',
    'TEMP_HIT_KEY': 'ym:pv:watchID UInt64 watchID',

    'ATTRIBUTIONS': 'AUTOMATIC CROSS_DEVICE_LAST CROSS_DEVICE_LAST_YANDEX_DIRECT_CLICK CROSS_DEVICE_FIRST CROSS_DEVICE_LAST_SIGNIFICANT',

    **dotenv_values('.env', verbose=False),
}

def env_value_or_error(key: str) -> str:
    val = __values[key]

    if val is None:
        raise Exception('UNDEFINED KEY')

    if val.strip() == '':
        raise Exception('UNDEFINED KEY')

    return val
