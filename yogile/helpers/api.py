import time

import requests

import helpers.env as env
from helpers.env import env_value_or_error, env_value_or_default


class YogileApiClient:
    MIN_REQUEST_INTERVAL = 0.3  # max ~3 req/s

    def __init__(self):
        self.base_url = env_value_or_default(env.YOGILE_BASE_URL, 'https://ru.yougile.com').rstrip('/')
        self._token = env_value_or_error(env.YOGILE_API_TOKEN)
        self._session = requests.Session()
        self._session.headers['Authorization'] = f'Bearer {self._token}'
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            time.sleep(self.MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _get(self, path: str, params: dict | None = None) -> dict:
        self._throttle()
        url = f'{self.base_url}{path}'
        response = self._session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_paginated(self, path: str, params: dict | None = None, page_size: int = 500) -> list[dict]:
        params = dict(params or {})
        params['limit'] = page_size
        params['offset'] = 0
        results: list[dict] = []
        while True:
            data = self._get(path, params)
            items: list[dict] = data.get('content', [])
            results.extend(items)
            if not data.get('paging', {}).get('next', False):
                break
            params['offset'] += len(items)
        return results

    def fetch_tasks(self) -> list[dict]:
        return self.fetch_paginated('/api-v2/task-list')

    def fetch_users(self) -> list[dict]:
        return self.fetch_paginated('/api-v2/users')

    def fetch_columns(self) -> list[dict]:
        return self.fetch_paginated('/api-v2/columns')

    def fetch_projects(self) -> list[dict]:
        return self.fetch_paginated('/api-v2/projects')

    def fetch_boards(self) -> list[dict]:
        return self.fetch_paginated('/api-v2/boards')

    def fetch_sprint_stickers(self) -> list[dict]:
        return self.fetch_paginated('/api-v2/sprint-stickers')

    def fetch_task(self, task_id: str) -> dict:
        return self._get(f'/api-v2/tasks/{task_id}')

    def fetch_string_sticker(self, sticker_id: str) -> dict:
        """Fetch a single string sticker with its states."""
        return self._get(f'/api-v2/string-stickers/{sticker_id}')
