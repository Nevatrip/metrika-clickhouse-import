import time
import requests


class BitrixApiClient:
    MIN_REQUEST_INTERVAL = 0.5  # max 2 req/s

    def __init__(self, webhook_url: str):
        self.base_url = webhook_url.rstrip('/')
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            time.sleep(self.MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _call(self, method: str, params: dict) -> dict:
        self._throttle()
        url = f"{self.base_url}/{method}.json"
        response = requests.post(url, json=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_list(self, method: str, params: dict, log_func=None) -> list[dict]:
        results = []
        start = 0
        while True:
            body = self._call(method, {**params, 'start': start})
            items = body.get('result', [])
            # tasks.task.list nests items under result.tasks
            if isinstance(items, dict) and 'tasks' in items:
                items = items['tasks']
            results.extend(items)
            if log_func:
                log_func(f"{method}: fetched {len(results)} records")
            next_start = body.get('next')
            if next_start is None:
                break
            start = next_start
        return results

    def fetch_leads(self, date_modify_from: str | None, log_func=None) -> list[dict]:
        params: dict = {'select': ['*', 'UF_*']}
        if date_modify_from:
            params['filter'] = {'>=DATE_MODIFY': date_modify_from}
        return self.fetch_list('crm.lead.list', params, log_func)

    def fetch_deals(self, date_modify_from: str | None, log_func=None) -> list[dict]:
        params: dict = {'select': ['*', 'UF_*']}
        if date_modify_from:
            params['filter'] = {'>=DATE_MODIFY': date_modify_from}
        return self.fetch_list('crm.deal.list', params, log_func)

    def fetch_fields(self, entity_type: str, log_func=None) -> dict:
        """crm.lead.fields / crm.deal.fields — возвращает метаданные всех полей."""
        body = self._call(f"crm.{entity_type}.fields", {})
        result = body.get('result', {})
        if log_func:
            log_func(f"crm.{entity_type}.fields: got {len(result)} fields")
        return result

    def fetch_statuses(self, log_func=None) -> list[dict]:
        return self.fetch_list('crm.status.list', {}, log_func)

    def fetch_stage_history(
        self,
        entity_type: str,
        date_from: str | None = None,
        log_func=None,
    ) -> list[dict]:
        method = f"crm.{entity_type}.stagehistory.list"
        params: dict = {}
        if date_from:
            params['filter'] = {'>=CREATED_DATE': date_from}
        try:
            return self.fetch_list(method, params, log_func)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 404):
                if log_func:
                    log_func(f"{method}: not available (HTTP {e.response.status_code}), skipping")
                return []
            raise

    def fetch_deal_categories(self, log_func=None) -> list[dict]:
        return self.fetch_list('crm.dealcategory.list', {}, log_func)

    def fetch_deal_category_stages(self, category_id: int, log_func=None) -> list[dict]:
        return self.fetch_list('crm.dealcategory.stage.list', {'id': category_id}, log_func)

    def fetch_tasks(self, date_modify_from: str | None, log_func=None) -> list[dict]:
        params: dict = {
            'select': ['ID', 'TITLE', 'STATUS', 'STATUS_CHANGED_DATE',
                       'RESPONSIBLE_ID', 'CREATED_DATE', 'CLOSED_DATE', 'UF_CRM_TASK'],
        }
        if date_modify_from:
            params['filter'] = {'>=CHANGED_DATE': date_modify_from}
        try:
            return self.fetch_list('tasks.task.list', params, log_func)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 401, 403, 404):
                if log_func:
                    log_func(f"tasks.task.list: not available (HTTP {e.response.status_code}), skipping")
                return []
            raise

    # --- Users / departments / worktime -----------------------------------

    def fetch_users(self, log_func=None) -> list[dict]:
        """Все сотрудники (активные и уволенные), без экстранета и ботов."""
        return self.fetch_list('user.get', {'FILTER': {'USER_TYPE': 'employee'}}, log_func)

    def fetch_departments(self, log_func=None) -> dict[int, str]:
        """{id: name}; пусто, если у вебхука нет права department."""
        try:
            body = self._call('department.get', {})
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 401, 403):
                if log_func:
                    log_func("department.get: no access, departments will be empty")
                return {}
            raise
        if 'error' in body:
            if log_func:
                log_func(f"department.get: {body['error']}, departments will be empty")
            return {}
        result = {}
        start = 0
        items = body.get('result', [])
        while True:
            for d in items:
                result[int(d['ID'])] = str(d.get('NAME') or '')
            if body.get('next') is None:
                break
            start = body['next']
            body = self._call('department.get', {'start': start})
            items = body.get('result', [])
        return result

    def fetch_workdays(self, user_id: int, year: int, month: int) -> list[dict]:
        """Рабочие дни сотрудника за месяц (timeman.timecontrol.reports.get)."""
        try:
            body = self._call('timeman.timecontrol.reports.get',
                              {'USER_ID': user_id, 'YEAR': year, 'MONTH': month})
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 403, 404):
                return []
            raise
        return ((body.get('result') or {}).get('report') or {}).get('days') or []

    def fetch_task_elapsed(self, log_func=None) -> list[dict]:
        """Все записи затраченного времени по задачам (task.elapseditem.getlist).
        Старый метод: пагинация через NAV_PARAMS, а не start."""
        results, page = [], 1
        while True:
            body = self._call('task.elapseditem.getlist', {
                'ORDER': {'ID': 'asc'},
                'FILTER': {},
                'SELECT': ['*'],
                'PARAMS': {'NAV_PARAMS': {'nPageSize': 50, 'iNumPage': page}},
            })
            items = body.get('result') or []
            results.extend(items)
            if log_func:
                log_func(f"task.elapseditem.getlist: fetched {len(results)} records")
            if not items or len(results) >= int(body.get('total') or 0):
                break
            page += 1
        return results

    def fetch_task_titles(self, task_ids: list[int], log_func=None) -> dict[int, str]:
        titles: dict[int, str] = {}
        for i in range(0, len(task_ids), 50):
            chunk = task_ids[i:i + 50]
            for t in self.fetch_list('tasks.task.list',
                                     {'filter': {'ID': chunk}, 'select': ['ID', 'TITLE']}, log_func):
                titles[int(t['id'])] = str(t.get('title') or '')
        return titles

    def fetch_tasks_changed(self, changed_from: str | None, log_func=None) -> list[dict]:
        params: dict = {'select': ['ID', 'TITLE', 'RESPONSIBLE_ID']}
        if changed_from:
            params['filter'] = {'>=CHANGED_DATE': changed_from}
        return self.fetch_list('tasks.task.list', params, log_func)

    def fetch_tasks_by_ids(self, task_ids: list[int], log_func=None) -> list[dict]:
        res = []
        for i in range(0, len(task_ids), 50):
            res += self.fetch_list('tasks.task.list', {
                'filter': {'ID': task_ids[i:i + 50]}, 'select': ['ID', 'TITLE', 'RESPONSIBLE_ID'],
            }, log_func)
        return res

    def fetch_task_status_history(self, task_id: int) -> list[dict]:
        """Смены статуса задачи по возрастанию времени: [{at, from, to, user_id}]."""
        body = self._call('tasks.task.history.list', {'taskId': task_id})
        items = ((body.get('result') or {}).get('list')) or []
        res = []
        for x in items:
            if x.get('field') != 'STATUS':
                continue
            user = x.get('user') if isinstance(x.get('user'), dict) else {}
            res.append({
                'at': x.get('createdDate'),
                'from': (x.get('value') or {}).get('from'),
                'to': (x.get('value') or {}).get('to'),
                'user_id': user.get('id'),
            })
        return sorted(res, key=lambda r: r['at'] or '')
