"""Описание выгружаемых таблиц Logs API AppMetrica.

Ключ — имя ресурса (https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints),
date_field — поле даты, по которому таблица партиционируется и перезаливается по дням,
fields — поля выгрузки. Все поля *_datetime хранятся как DateTime, остальное — String,
чтобы не падать на пустых значениях и неожиданных форматах.
"""

DEVICE_FIELDS = [
    'appmetrica_device_id', 'installation_id', 'profile_id',
    'google_aid', 'ios_ifa', 'ios_ifv',
    'os_name', 'os_version', 'app_version_name', 'app_build_number',
    'device_manufacturer', 'device_model', 'device_type', 'device_locale',
    'connection_type', 'operator_name', 'country_iso_code', 'city',
]

SOURCES: dict[str, dict] = {
    'events': {
        'date_field': 'event_datetime',
        'order_by': ['event_datetime', 'appmetrica_device_id'],
        'fields': [
            'event_datetime', 'event_receive_datetime',
            'event_name', 'event_json', 'session_id',
            *DEVICE_FIELDS,
        ],
    },
    'sessions_starts': {
        'date_field': 'session_start_datetime',
        'order_by': ['session_start_datetime', 'appmetrica_device_id'],
        'fields': [
            'session_start_datetime', 'session_start_receive_datetime', 'session_id',
            *DEVICE_FIELDS,
        ],
    },
    'installations': {
        'date_field': 'install_datetime',
        'order_by': ['install_datetime', 'appmetrica_device_id'],
        'fields': [
            'install_datetime', 'install_receive_datetime',
            'is_reinstallation', 'is_reattribution', 'attributed_touch_type', 'match_type',
            'tracker_name', 'tracking_id', 'publisher_id', 'publisher_name',
            'click_datetime', 'click_id', 'click_url_parameters',
            *DEVICE_FIELDS,
        ],
    },
}

TZ = 'Europe/Moscow'  # часовой пояс приложения в AppMetrica


def column_type(field: str) -> str:
    return f"DateTime('{TZ}')" if field.endswith('_datetime') else 'String'
