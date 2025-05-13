from dotenv import dotenv_values

values = {
    'CLICKHOUSE_HOST': 'localhost',
    'CLICKHOUSE_USER': 'root',
    'CLICKHOUSE_PASSWORD': 'secret',
    'CLICKHOUSE_DATABASE': 'metrika',

    'METRIKA_COUNTER': None,
    'METRIKA_KEY': None,

    'METRIKA_CHAR_LIMIT': 3000,

    'TEMP_HIT_TABLE_PREFIX': 'temp_hits',
    'HIT_TABLE_PREFIX': 'hits',

    'TEMP_VISIT_TABLE_PREFIX': 'temp_visits',
    'VISIT_TABLE_PREFIX': 'visits',

    'TEMP_VISIT_KEY': 'ym:s:visitID UInt64 visitID',
    'TEMP_HIT_KEY': 'ym:pv:watchID UInt64 watchID',

    **dotenv_values('.env', verbose=False),
}
