from dotenv import dotenv_values
import os

CLICKHOUSE_HOST           = 'CLICKHOUSE_HOST'
CLICKHOUSE_USER           = 'CLICKHOUSE_USER'
CLICKHOUSE_PASSWORD       = 'CLICKHOUSE_PASSWORD'
CLICKHOUSE_DATABASE       = 'CLICKHOUSE_DATABASE'
YOGILE_BASE_URL           = 'YOGILE_BASE_URL'
YOGILE_API_TOKEN          = 'YOGILE_API_TOKEN'
YOGILE_STICKER_SP_FRONTEND = 'YOGILE_STICKER_SP_FRONTEND'
YOGILE_STICKER_SP_BACKEND  = 'YOGILE_STICKER_SP_BACKEND'
YOGILE_STICKER_PROJECT     = 'YOGILE_STICKER_PROJECT'
YOGILE_STICKER_SPRINT      = 'YOGILE_STICKER_SPRINT'
YOGILE_SPRINT_START        = 'YOGILE_SPRINT_START'
YOGILE_SPRINT_LENGTH_DAYS  = 'YOGILE_SPRINT_LENGTH_DAYS'
YOGILE_CARDS_FROM          = 'YOGILE_CARDS_FROM'
YOGILE_PROJECTS            = 'YOGILE_PROJECTS'

__values: dict[str, str | None] = {
    CLICKHOUSE_HOST:           'localhost',
    CLICKHOUSE_USER:           'default',
    CLICKHOUSE_PASSWORD:       'secret',
    CLICKHOUSE_DATABASE:       'yogile',
    YOGILE_BASE_URL:           'https://ru.yougile.com',
    YOGILE_API_TOKEN:          None,
    YOGILE_STICKER_SP_FRONTEND: None,
    YOGILE_STICKER_SP_BACKEND:  None,
    YOGILE_STICKER_PROJECT:     None,
    YOGILE_STICKER_SPRINT:      None,
    YOGILE_SPRINT_START:        None,
    YOGILE_SPRINT_LENGTH_DAYS:  '14',
    YOGILE_CARDS_FROM:          None,
    YOGILE_PROJECTS:            None,  # required
    **dotenv_values(os.path.dirname(os.path.realpath(__file__)) + '/../.env', verbose=False),
}


def env_value_or_error(key: str) -> str:
    val = __values.get(key)
    if not val or not val.strip():
        raise Exception(f'ENV key not set: {key}')
    return val.strip()


def env_value_or_default(key: str, default: str = '') -> str:
    val = __values.get(key)
    if not val or not val.strip():
        return default
    return val.strip()
