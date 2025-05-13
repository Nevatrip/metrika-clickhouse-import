# Импорт Яндекс метрики в clickhouse

## Ссылки на докуменацию

- API передачи параметров <https://yandex.ru/support/metrica/ru>
- Logs API <https://yandex.ru/dev/metrika/ru/logs>
- Официальная библиотека для python <https://clickhouse.com/docs/integrations/python>
  - Используется для импорта файлов метрики, так как тут это удобнее и понятнее, чем в других библиотеках
- Неофициальная библиотека для python <https://clickhouse-driver.readthedocs.io>
  - Пока не используется, но поддерживает нативный протокол сервера clickhouse. Потенциально для создания отчётов быстрее, чем по HTTP протоколу
- Вся остальная документация clickhouse <https://clickhouse.com/docs>

## Получение токена

Тут написано как создать приложение и получить токен <https://yandex.ru/dev/metrika/ru/intro/authorization>

Дальше просто запульнуть его в `.env` вместе с id счётчика

## Ок, у меня есть куча метрики в clickhouse, дальше что?

Запрос на создание виртуальной таблицы mysql в clickhouse
<https://clickhouse.com/docs/engines/table-engines/integrations/mysql>

```dql
CREATE TABLE <virtual_mysql_table> ENGINE = MySQL('<ip>:<port>', <mysql_database>, <mysql_table>, <mysql_user>, <mysql_password>)
```

После объединить стандартным JOIN

```dql
SELECT * FROM <clickhouse_table> JOIN <virtual_mysql_table> ON <clickhouse_table>.<clickhouse_id> = <virtual_mysql_table>.<mysql_id>
```

Виртуальная таблица "обновляется" в реальном времени, пересоздавать не нужно.

Есть так же возможность подрубить сразу всю базу MySQL, а не только одну таблицу
<https://clickhouse.com/docs/engines/database-engines/mysql>

## Известные приколы API или почему всё так странно работает?

Из-за ограничения на количество символов в поле `fields` при создании запроса логов запрос приходится разбивать на 2 запроса.
Из-за этого сначала скачивается одна часть полей и импортируется во временную таблицу с первичным ключём `visitID` (или `watchID` для событий).
После этого скачивается вторая часть полей и вставляется во вторую временную таблицу с первичным ключём `visitID` (или `watchID` для событий).
После они объединяются `JOIN`'ом и вставляются в конечную таблицу.

Реализованы функции как для загрузки метрики из файлов напрямую, так и через временные таблицы.
Имя первичного ключа задаётся переменными окружения `TEMP_VISIT_KEY` и `TEMP_HIT_KEY` (для визитов и событий соответственно) в формате `<metrika_field_name> <field_type> <clickhouse_field_name>`.
Обычно её менять не нужно, просто на всякий случай, чтобы была кастомизация.
