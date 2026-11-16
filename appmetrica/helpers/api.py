import time
from collections.abc import Callable

import requests as rq

BASE_URL = 'https://api.appmetrica.yandex.ru/logs/v1/export'


def export_to_file(
    token: str,
    app_id: str,
    source: str,
    fields: list[str],
    date_since: str,
    date_until: str,
    path: str,
    poll_seconds: int,
    poll_timeout: int,
    log: Callable[[str], None],
) -> None:
    """Скачать выгрузку Logs API в CSV-файл.

    Пока выгрузка готовится, API отвечает 202 — повторяем тот же запрос
    (с теми же параметрами, иначе подготовка начнётся заново) до 200.
    """
    params = {
        'application_id': app_id,
        'date_since': date_since,
        'date_until': date_until,
        'date_dimension': 'default',
        'use_utf8_bom': 'false',
        'fields': ','.join(fields),
    }
    headers = {'Authorization': f'OAuth {token}'}
    deadline = time.time() + poll_timeout

    while True:
        with rq.get(f'{BASE_URL}/{source}.csv', params=params, headers=headers, stream=True, timeout=600) as r:
            if r.status_code == 200:
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                return

            body = r.text[:500]
            if r.status_code not in (202, 429):
                raise Exception(f'{source} {date_since}: HTTP {r.status_code}: {body}')

        if time.time() > deadline:
            raise Exception(f'{source} {date_since}: timeout waiting for export ({body})')

        log(f'{source} {date_since}: HTTP {r.status_code} {body.strip()}')
        time.sleep(poll_seconds)
