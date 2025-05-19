def create(counter: str|int):
    return f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequests"

def check(counter: str|int, request_id: str|int):
    return f"http://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}"

def download(counter: str|int, request_id: str|int, part: str|int):
    return f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/part/{part}/download"

def clean(counter: str|int, request_id: str|int):
    return f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/clean"

