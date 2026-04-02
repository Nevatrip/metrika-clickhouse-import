# Bitrix24 -> ClickHouse ETL

Импорт данных CRM из Bitrix24 в ClickHouse.
Запускается по крону в 03:00. Инкрементальный: каждый запуск подтягивает только изменённые записи (по `DATE_MODIFY`).

## Быстрый старт локально

```bash
cp .env.example .env  # заполнить переменные
python init.py        # создать таблицы (один раз)
python insert.py      # запустить импорт
```

### Где взять BITRIX_WEBHOOK_URL

1. Войти в Bitrix24 → **Разработчикам** (или `/devops/`) → **Входящий вебхук**
2. Создать вебхук, выдать права: **CRM** (чтение) + **Задачи** (чтение)
3. Скопировать URL вида `https://your-company.bitrix24.ru/rest/1/<token>/`

## Таблицы

### `bitrix_leads`
Лиды CRM. Инкрементальный sync по `DATE_MODIFY`.

Ключевые поля: `ID`, `TITLE`, `STATUS_ID`, `SOURCE_ID`, `DATE_CREATE`, `DATE_MODIFY`, `ASSIGNED_BY_ID`, `UTM_*`, `PHONE`, `EMAIL`, `OPPORTUNITY`, `raw_data` (полный JSON).  
Дополнительно: все кастомные поля `UF_CRM_*` как отдельные колонки (с комментариями).

---

### `bitrix_deals`
Сделки CRM. Инкрементальный sync по `DATE_MODIFY`.

Ключевые поля: `ID`, `TITLE`, `STAGE_ID`, `TYPE_ID`, `SOURCE_ID`, `DATE_CREATE`, `DATE_MODIFY`, `CONTACT_ID`, `LEAD_ID`, `OPPORTUNITY`, `UTM_*`, `raw_data`.  
Дополнительно: все кастомные поля `UF_CRM_*` как отдельные колонки (с комментариями).

---

### `bitrix_statuses`
Справочник статусов и стадий. Полностью обновится при каждом запуске.

Поля: `ENTITY_ID` (тип справочника), `STATUS_ID` (код), `NAME` (название), `SORT`, `COLOR`.

Используется для расшифровки `STATUS_ID` в лидах и `STAGE_ID` в сделках.

---

### `bitrix_fields`
Метаданные всех полей лидов и сделок. Полностью обновится при каждом запуске.

Поля: `entity_type` (lead/deal), `field_id`, `title` (человекочитаемое название), `field_type`, `is_multiple`, `is_required`.

---

### `bitrix_enum_values`
Возможные значения полей типа enum. Полностью обновится при каждом запуске.

Поля: `entity_type`, `field_id`, `item_id`, `item_value`.

---

### `bitrix_checkpoint`
Хранит дату последнего успешного запуска по каждой сущности.
