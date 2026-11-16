from dotenv import dotenv_values
import os

CLICKHOUSE_HOST      = 'CLICKHOUSE_HOST'
CLICKHOUSE_USER      = 'CLICKHOUSE_USER'
CLICKHOUSE_PASSWORD  = 'CLICKHOUSE_PASSWORD'
CLICKHOUSE_DATABASE  = 'CLICKHOUSE_DATABASE'
APPMETRICA_TOKEN     = 'APPMETRICA_TOKEN'
APPMETRICA_APP_ID    = 'APPMETRICA_APP_ID'
APPMETRICA_SOURCES   = 'APPMETRICA_SOURCES'
FIRST_DATE           = 'FIRST_DATE'
DAY_COUNT            = 'DAY_COUNT'
RELOAD_DAYS          = 'RELOAD_DAYS'
POLL_SECONDS         = 'POLL_SECONDS'
POLL_TIMEOUT         = 'POLL_TIMEOUT'

__values: dict[str, str | None] = {
    CLICKHOUSE_HOST:     'localhost',
    CLICKHOUSE_USER:     'default',
    CLICKHOUSE_PASSWORD: 'secret',
    CLICKHOUSE_DATABASE: 'appmetrica',
    APPMETRICA_TOKEN:    None,
    APPMETRICA_APP_ID:   None,
    APPMETRICA_SOURCES:  'events installations sessions_starts',
    FIRST_DATE:          None,
    DAY_COUNT:           '30',
    RELOAD_DAYS:         '7',
    POLL_SECONDS:        '30',
    POLL_TIMEOUT:        '3600',
    **dotenv_values(os.path.dirname(os.path.realpath(__file__)) + '/../.env', verbose=False),
}


def env_value_or_error(key: str) -> str:
    val = __values.get(key)
    if not val or not val.strip():
        raise Exception(f'ENV key not set: {key}')
    return val.strip()
