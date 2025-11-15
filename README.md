# Работа с Kafka 

## Описание:
В базе данных `PostgreSQL` хранится таблица `user_logins`. </br>
В ней содержатся события пользователей, такие как логин, регистрация, покупка и т.д. </br>
Каждый раз, когда необходимо перенести эти события из `PostgreSQL` в другую систему (например, `ClickHouse`), </br>
можно воспользоваться `Kafka` как промежуточным звеном для передачи сообщений. </br> 
Однако, в реальных задачах возникает риск повторной отправки уже обработанных данных. </br>
Чтобы избежать дублирования, нужно использовать дополнительное логическое поле в таблице — `sent_to_kafka - BOOLEAN`, </br>
которое будет сигнализировать, были ли данные уже отправлены в `Kafka`.</br>

## Реализация:
Для работы нам понадобятся `PostgreSQL` -> `Kafka` -> `ClickHouse`. <br>
В проекте используются следующие скрипты:
- `producer_pg_to_kafka.py` - скрипт, который отправляет данные в **Kafka**.
- `consumer_to_clickhouse.py` - скрипт, который читает данные из **Kafka**.

***1) Создаем таблицу `user_logins` в `PostgreSQL` со следующей структурой:*** 
```
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,                -- Создан идентификатор записи
    username TEXT,                        -- Имя пользователя
    event_type TEXT,                      -- Событие, совершенное пользователем
    event_time TIMESTAMP,                 -- Время, когда было совершено событие
    sent_to_kafka BOOLEAN DEFAULT FALSE   -- Создан стобец sent_to_kafka, который по умолчанию принимает значение FALSE
)
```
- Столбец `sent_to_kafka BOOLEAN` сигнализирует, были ли данные уже отправлены в `Kafka`.
  
***2) Добавим тестовые данные в таблицу `user_logins`:***
```
insert into user_logins
(username, event_type, event_time) 
values
('alice',	'login',	'2025-11-12 13:53:51'),
('bob',	'signup',	'2025-11-12 13:53:52'),
('bob',	'purchase',	'2025-11-12 13:53:53'),
('carol',	'signup',	'2025-11-12 13:53:54'),
('dave',	'login',	'2025-11-12 13:53:55'),
('carol',	'purchase',	'2025-11-12 13:53:56'),
('carol',	'signup',	'2025-11-12 13:53:57'),
('bob',	'login',	'2025-11-12 13:53:58')
```
***3) Далее запускаем `producer_pg_to_kafka.py` 1 раз, он добавляет данные в `Kafka` и при этом флаг </br>
`sent_to_kafka` устанавливает для этих записей в значение **TRUE**.</br>***
```
D:\DE\DE_Kafka_project\.venv\Scripts\python.exe D:\DE\DE_Kafka_project\producer_pg_to_kafka.py 
Sent: {'id': 621, 'user': 'alice', 'event': 'login', 'timestamp': 1762955631.0}
Sent: {'id': 622, 'user': 'bob', 'event': 'signup', 'timestamp': 1762955632.0}
Sent: {'id': 623, 'user': 'bob', 'event': 'purchase', 'timestamp': 1762955633.0}
Sent: {'id': 624, 'user': 'carol', 'event': 'signup', 'timestamp': 1762955634.0}
Sent: {'id': 625, 'user': 'dave', 'event': 'login', 'timestamp': 1762955635.0}
Sent: {'id': 626, 'user': 'carol', 'event': 'purchase', 'timestamp': 1762955636.0}
Sent: {'id': 627, 'user': 'carol', 'event': 'signup', 'timestamp': 1762955637.0}
Sent: {'id': 628, 'user': 'bob', 'event': 'login', 'timestamp': 1762955638.0}

Process finished with exit code 0
```
***4) Следующим этапом запускаем запускаем  `consumer_to_clickhouse.py` первый раз. </br>***
```
D:\DE\DE_Kafka_project\.venv\Scripts\python.exe D:\DE\DE_Kafka_project\consumer_to_clickhouse.py 
Received: {'id': 621, 'user': 'alice', 'event': 'login', 'timestamp': 1762955631.0}
Received: {'id': 622, 'user': 'bob', 'event': 'signup', 'timestamp': 1762955632.0}
Received: {'id': 623, 'user': 'bob', 'event': 'purchase', 'timestamp': 1762955633.0}
Received: {'id': 624, 'user': 'carol', 'event': 'signup', 'timestamp': 1762955634.0}
Received: {'id': 625, 'user': 'dave', 'event': 'login', 'timestamp': 1762955635.0}
Received: {'id': 626, 'user': 'carol', 'event': 'purchase', 'timestamp': 1762955636.0}
Received: {'id': 627, 'user': 'carol', 'event': 'signup', 'timestamp': 1762955637.0}
Received: {'id': 628, 'user': 'bob', 'event': 'login', 'timestamp': 1762955638.0}
```
Он получает данные из `Kafka` и сохраняет их в `ClickHouse` в таблицу `user_logins` со следующей структурой:
```
CREATE TABLE IF NOT EXISTS user_logins (
    id UInt32,
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
```

***5) Далее мы добавляем в таблицу `user_logins` в `PostgreSQL` еще одну порцию данных:***
```
insert into user_logins
(username, event_type, event_time) 
values
('bob',	'signup',	'2025-11-12 19:55:58'),
('carol',	'login',	'2025-11-12 19:55:57'),
('carol',	'purchase',	'2025-11-12 19:55:56')
```
- чтобы убедиться, что продюсер не отправляет повторно записи и флаг `sent_to_kafka` корректно выставлен. </br>

***6) Далее снова запускаем `producer_pg_to_kafka.py` 2 раз. </br>***
```
D:\DE\DE_Kafka_project\.venv\Scripts\python.exe D:\DE\DE_Kafka_project\producer_pg_to_kafka.py 
Sent: {'id': 629, 'user': 'bob', 'event': 'signup', 'timestamp': 1762977358.0}
Sent: {'id': 630, 'user': 'carol', 'event': 'login', 'timestamp': 1762977357.0}
Sent: {'id': 631, 'user': 'carol', 'event': 'purchase', 'timestamp': 1762977356.0}

Process finished with exit code 0
```
***7) Следующим этапом запускаем запускаем `consumer_to_clickhouse.py` второй раз, он получает данные из `Kafka` и сохраняет их в `ClickHouse`.</br>***
```
D:\DE\DE_Kafka_project\.venv\Scripts\python.exe D:\DE\DE_Kafka_project\consumer_to_clickhouse.py 
Received: {'id': 629, 'user': 'bob', 'event': 'signup', 'timestamp': 1762977358.0}
Received: {'id': 630, 'user': 'carol', 'event': 'login', 'timestamp': 1762977357.0}
Received: {'id': 631, 'user': 'carol', 'event': 'purchase', 'timestamp': 1762977356.0}
```

***8) В таблицу `user_logins` в `ClickHouse` добавилось только 3 записи. Id записей могут отличаться от примера. </br>***
`Producer_pg_to_kafka.py и `consumer_to_clickhouse.py` работают корректно. </br>
В результате реализации получилось устойчивое решение миграции данных с защитой от дубликатов.
