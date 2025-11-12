# Работа с Kafka 

## Описание:
В базе данных `PostgreSQL` хранится таблица `user_logins`. </br>
В ней содержатся события пользователей, такие как логин, регистрация, покупка и т.д. </br>
Каждый раз, когда необходимо перенести эти события из `PostgreSQL` в другую систему (например, `ClickHouse`), </br>
можно воспользоваться `Kafka` как промежуточным звеном для передачи сообщений. </br> 
Однако, в реальных задачах возникает риск повторной отправки уже обработанных данных. </br>
Чтобы избежать дублирования, нужно использовать дополнительное логическое поле в таблице — `sent_to_kafka BOOLEAN`, </br>
которое будет сигнализировать, были ли данные уже отправлены в `Kafka`.</br>
## Реализация:
1) Создана таблица `user_logins` в `PostgreSQL` со следующей структурой: 
```
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,                    -- Создан идентификатор записи
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE      -- Создан стобец sent_to_kafka, который по умолчанию принимает значение **FALSE**
)
```
- Столбец `sent_to_kafka BOOLEAN` сигнализирует, были ли данные уже отправлены в `Kafka`.
  
2) Добавим тестовые данные в таблицу `user_logins`:
```
insert into user_logins
(username, event_type, event_time) 
values
('alice',	'login',	'2025-11-12 13:53:51'),
('bob',	'login',	'2025-11-12 13:53:52'),
('bob',	'login',	'2025-11-12 13:53:53'),
('carol',	'login',	'2025-11-12 13:53:54'),
('dave',	'login',	'2025-11-12 13:53:55'),
('carol',	'login',	'2025-11-12 13:53:56'),
('carol',	'login',	'2025-11-12 13:53:57'),
('bob',	'login',	'2025-11-12 13:53:58')
```
3) Далее запускаем продюсер `producer_pg_to_kafka.py` 1 раз, он добавляет данные в Kafka и при этом флаг </br>
`sent_to_kafka` устанавливает для этих записей в значение **TRUE**.
4) Следующим этапом запускаем запускаем консьмер `consumer_to_clickhouse.py` первый раз, </br>
он получает данные из Kafka и сохраняет их в ClickHouse в таблицу `user_logins` со следующей структурой:
```
CREATE TABLE IF NOT EXISTS user_logins (
    id UInt32,
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
```  
5) Далее мы добавляем в таблицу `user_logins` в `PostgreSQL` еще одну порцию данных:
```
insert into user_logins
(username, event_type, event_time) 
values
('bob',	'login',	'2025-11-12 19:55:58'),
('carol',	'login',	'2025-11-12 19:55:57'),
('carol',	'login',	'2025-11-12 19:55:56')
```
- чтобы убедиться, продюсер не отправляет повторно записи и флаг `sent_to_kafka` корректно выставлен.
6) Далее снова запускаем продюсер `producer_pg_to_kafka.py` 2 раз.
7) Следующим этапом запускаем запускаем консьмер `consumer_to_clickhouse.py` второй раз, он получает данные из Kafka и сохраняет в ClickHouse.
8) В таблицу `user_logins` в `ClickHouse` добавилось только 3 записи. Продюсер и консьмер работают корректно. <\br>
В результате реализации получится устойчивое решение миграции данных с защитой от дубликатов.
