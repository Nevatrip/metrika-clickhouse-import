from dotenv import dotenv_values
import os

CLICKHOUSE_HOST     = 'CLICKHOUSE_HOST'
CLICKHOUSE_USER     = 'CLICKHOUSE_USER'
CLICKHOUSE_PASSWORD = 'CLICKHOUSE_PASSWORD'
CLICKHOUSE_DATABASE = 'CLICKHOUSE_DATABASE'
BITRIX_WEBHOOK_URL  = 'BITRIX_WEBHOOK_URL'

__values: dict[str, str | None] = {
    CLICKHOUSE_HOST:     'localhost',
    CLICKHOUSE_USER:     'default',
    CLICKHOUSE_PASSWORD: 'secret',
    CLICKHOUSE_DATABASE: 'bitrix',
    BITRIX_WEBHOOK_URL:  None,
    **dotenv_values(os.path.dirname(os.path.realpath(__file__)) + '/../.env', verbose=False),
}

def env_value_or_error(key: str) -> str:
    val = __values.get(key)
    if not val or not val.strip():
        raise Exception('UNDEFINED KEY')
    return val.strip()
