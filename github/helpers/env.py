from dotenv import dotenv_values
import os

CLICKHOUSE_HOST         = 'CLICKHOUSE_HOST'
CLICKHOUSE_USER         = 'CLICKHOUSE_USER'
CLICKHOUSE_PASSWORD     = 'CLICKHOUSE_PASSWORD'
CLICKHOUSE_DATABASE     = 'CLICKHOUSE_DATABASE'
GITHUB_TOKEN            = 'GITHUB_TOKEN'
GITHUB_OWNER            = 'GITHUB_OWNER'
GITHUB_REPOS            = 'GITHUB_REPOS'
GITHUB_BRANCH           = 'GITHUB_BRANCH'
GITHUB_COMMITS_FROM     = 'GITHUB_COMMITS_FROM'
GITHUB_MAX_DETAIL_CALLS = 'GITHUB_MAX_DETAIL_CALLS'

__values: dict[str, str | None] = {
    CLICKHOUSE_HOST:         'localhost',
    CLICKHOUSE_USER:         'default',
    CLICKHOUSE_PASSWORD:     'secret',
    CLICKHOUSE_DATABASE:     'github',
    GITHUB_TOKEN:            None,
    GITHUB_OWNER:            'Nevatrip',
    GITHUB_REPOS:            None,  # required
    GITHUB_BRANCH:           'main',
    GITHUB_COMMITS_FROM:     None,
    GITHUB_MAX_DETAIL_CALLS: '3000',
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
