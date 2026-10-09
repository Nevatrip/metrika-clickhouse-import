#!/usr/bin/env python3
"""
Инкрементальная выгрузка коммитов ветки (main) из GitHub в ClickHouse.

Берётся только first-parent цепочка ветки: каждый squash-merge / прямой коммит
в main — одна строка. Коммиты из веток, влитых merge-коммитом, не попадают
(у самого merge-коммита parents_count > 1).

Для каждого нового коммита дополнительно запрашивается список файлов
(1 запрос к API на коммит). Если лимит запросов за запуск исчерпан —
скрипт сохраняет то, что успел, и продолжит со следующего запуска.
"""

import re
from datetime import datetime, timezone

import clickhouse_connect as cc
from clickhouse_driver import Client

import helpers.env as env
from helpers.api import GithubApiClient, RateLimitExhausted
from helpers.env import env_value_or_error, env_value_or_default

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def log(s: str) -> None:
    print(datetime.now().isoformat(), s)

ch_query = Client(
    host=env_value_or_error(env.CLICKHOUSE_HOST),
    user=env_value_or_error(env.CLICKHOUSE_USER),
    password=env_value_or_error(env.CLICKHOUSE_PASSWORD),
)

insert_client = cc.get_client(
    host=env_value_or_error(env.CLICKHOUSE_HOST),
    username=env_value_or_error(env.CLICKHOUSE_USER),
    password=env_value_or_error(env.CLICKHOUSE_PASSWORD),
)

db = env_value_or_error(env.CLICKHOUSE_DATABASE)
api = GithubApiClient()

owner = env_value_or_default(env.GITHUB_OWNER, 'Nevatrip')
branch = env_value_or_default(env.GITHUB_BRANCH, 'main')
repos = [r.strip() for r in env_value_or_error(env.GITHUB_REPOS).split(',') if r.strip()]
commits_from = env_value_or_default(env.GITHUB_COMMITS_FROM)  # YYYY-MM-DD или пусто = вся история
since = f'{commits_from}T00:00:00Z' if commits_from else None
detail_budget = int(env_value_or_default(env.GITHUB_MAX_DETAIL_CALLS, '3000'))

BATCH_SIZE = 100

COMMIT_COLUMNS = [
    'repo', 'sha', 'committed_at', 'authored_at', 'author_login', 'author_name',
    'subject', 'message', 'prefix', 'scope', 'task_id', 'parents_count',
    'additions', 'deletions', 'files_count',
]
FILE_COLUMNS = ['repo', 'sha', 'filename', 'status', 'additions', 'deletions']

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

# fix: …, fix(TECH-2903): …, feat(scope)!: …
PREFIX_RE = re.compile(r'^\s*([A-Za-z]+)\s*(?:\(([^)]*)\))?\s*!?\s*:')
TASK_RE = re.compile(r'\b(TECH-\d+)\b', re.IGNORECASE)


def parse_subject(subject: str) -> tuple[str, str, str]:
    """Returns (prefix, scope, task_id)."""
    prefix, scope = '', ''
    m = PREFIX_RE.match(subject)
    if m:
        prefix = m.group(1).lower()
        scope = (m.group(2) or '').strip()

    task_id = ''
    t = TASK_RE.search(scope) or TASK_RE.search(subject)
    if t:
        task_id = t.group(1).upper()
    return prefix, scope, task_id


def parse_dt(s: str | None) -> datetime:
    if not s:
        return datetime(1970, 1, 1)
    return datetime.fromisoformat(s.replace('Z', '+00:00')).astimezone(timezone.utc).replace(tzinfo=None)

# ---------------------------------------------------------------------------
# Per-repo sync
# ---------------------------------------------------------------------------

def known_shas(repo: str) -> set[str]:
    rows = ch_query.execute(f"SELECT sha FROM {db}.commits WHERE repo = %(repo)s", {'repo': repo})
    return {r[0] for r in rows}


def new_first_parent_chain(repo: str, known: set[str]) -> list[dict]:
    """Walks first-parent chain from branch head until a known commit (or `since`). Oldest first."""
    by_sha: dict[str, dict] = {}
    chain: list[dict] = []
    cur: str | None = None

    for page in api.list_commits_pages(owner, repo, branch, since):
        for item in page:
            by_sha[item['sha']] = item
        if cur is None:
            cur = page[0]['sha']

        while cur and cur in by_sha and cur not in known:
            item = by_sha[cur]
            chain.append(item)
            parents = item.get('parents') or []
            cur = parents[0]['sha'] if parents else None

        if cur is None or cur in known:
            break
        # cur ещё не загружен — нужна следующая страница

    chain.reverse()
    return chain


def flush(commit_rows: list, file_rows: list) -> None:
    # Сначала файлы, потом коммит: если упадёт посередине — коммит перезагрузится в следующий раз
    if file_rows:
        insert_client.insert(f'{db}.commit_files', file_rows, column_names=FILE_COLUMNS)
    if commit_rows:
        insert_client.insert(f'{db}.commits', commit_rows, column_names=COMMIT_COLUMNS)
    commit_rows.clear()
    file_rows.clear()


def sync_repo(repo: str) -> int:
    global detail_budget

    known = known_shas(repo)
    chain = new_first_parent_chain(repo, known)
    log(f'  {repo}: {len(known)} known, {len(chain)} new commits on {branch}')

    commit_rows: list = []
    file_rows: list = []
    inserted = 0

    try:
        for item in chain:
            if detail_budget <= 0:
                log(f'  {repo}: detail budget exhausted, will continue next run')
                break

            detail = api.get_commit(owner, repo, item['sha'])
            detail_budget -= 1

            c = detail.get('commit', {})
            message = c.get('message', '')
            subject = message.split('\n', 1)[0]
            prefix, scope, task_id = parse_subject(subject)
            files = detail.get('files') or []
            stats = detail.get('stats') or {}

            commit_rows.append((
                repo,
                detail['sha'],
                parse_dt((c.get('committer') or {}).get('date')),
                parse_dt((c.get('author') or {}).get('date')),
                (detail.get('author') or {}).get('login', '') or '',
                (c.get('author') or {}).get('name', '') or '',
                subject,
                message,
                prefix,
                scope,
                task_id,
                len(detail.get('parents') or []),
                int(stats.get('additions', 0)),
                int(stats.get('deletions', 0)),
                len(files),
            ))
            for f in files:
                file_rows.append((
                    repo,
                    detail['sha'],
                    f.get('filename', ''),
                    f.get('status', ''),
                    int(f.get('additions', 0)),
                    int(f.get('deletions', 0)),
                ))

            if len(commit_rows) >= BATCH_SIZE:
                inserted += len(commit_rows)
                flush(commit_rows, file_rows)
    finally:
        inserted += len(commit_rows)
        flush(commit_rows, file_rows)

    log(f'  {repo}: inserted {inserted} commits')
    return inserted

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

log(f'Syncing {len(repos)} repos ({owner}, branch {branch}, since {commits_from or "beginning"})')
total = 0
for repo in repos:
    try:
        total += sync_repo(repo)
    except RateLimitExhausted as e:
        log(f'  STOP: {e}. Will continue next run')
        break
    except Exception as e:  # один репозиторий не должен ронять остальные
        log(f'  ERROR in {repo}: {e}')

log(f'Done: inserted {total} commits, API remaining: {api.remaining}')
