import time

import requests

import helpers.env as env
from helpers.env import env_value_or_error


class RateLimitExhausted(Exception):
    pass


class GithubApiClient:
    BASE_URL = 'https://api.github.com'
    MIN_REMAINING = 50  # оставляем запас лимита, чтобы не упереться в 403

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {env_value_or_error(env.GITHUB_TOKEN)}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        })
        self.remaining: int | None = None

    def _get(self, path: str, params: dict | None = None) -> requests.Response:
        if self.remaining is not None and self.remaining < self.MIN_REMAINING:
            raise RateLimitExhausted(f'GitHub rate limit almost exhausted ({self.remaining} left)')

        for attempt in range(3):
            response = self._session.get(f'{self.BASE_URL}{path}', params=params, timeout=30)
            remaining = response.headers.get('X-RateLimit-Remaining')
            if remaining is not None:
                self.remaining = int(remaining)

            if response.status_code in (403, 429) and self.remaining == 0:
                raise RateLimitExhausted('GitHub rate limit exhausted')
            if response.status_code >= 500 and attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            response.raise_for_status()
            return response

        response.raise_for_status()
        return response

    def list_commits_pages(self, owner: str, repo: str, branch: str, since: str | None):
        """Yields pages (lists) of commits of branch, newest first."""
        params: dict = {'sha': branch, 'per_page': 100, 'page': 1}
        if since:
            params['since'] = since
        while True:
            items = self._get(f'/repos/{owner}/{repo}/commits', params).json()
            if not items:
                return
            yield items
            if len(items) < params['per_page']:
                return
            params['page'] += 1

    def get_commit(self, owner: str, repo: str, sha: str) -> dict:
        return self._get(f'/repos/{owner}/{repo}/commits/{sha}').json()
