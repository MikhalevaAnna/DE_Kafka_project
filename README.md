# <img width="25" height="25" alt="image" src="https://github.com/user-attachments/assets/837618d8-a6e2-4a4a-9e70-1f8108c831c5" /> Работа с Kafka 

## <img width="25" height="25" alt="image" src="https://github.com/user-attachments/assets/98694e2d-f7f1-4318-8466-b224d1905dc6" /> Описание проекта:
В базе данных `PostgreSQL` хранится таблица `user_logins`. </br>
В ней содержатся события пользователей, такие как логин, регистрация, покупка и т.д. </br>
Каждый раз, когда необходимо перенести эти события из `PostgreSQL` в другую систему (например, `ClickHouse`), </br>
можно воспользоваться `Kafka` как промежуточным звеном для передачи сообщений. </br> 
Однако, в реальных задачах возникает риск повторной отправки уже обработанных данных. </br>
Чтобы избежать дублирования, нужно использовать дополнительное логическое поле в таблице — `sent_to_kafka - BOOLEAN`, </br>
которое будет сигнализировать, были ли данные уже отправлены в `Kafka`.</br>
## <img width="27" height="27" alt="image" src="https://github.com/user-attachments/assets/24ff5b7d-14bf-4503-bf99-877fb6528e9e" /> Что необходимо сделать для реализации:
Для работы нам понадобятся `PostgreSQL` -> `Kafka` -> `ClickHouse`. </br>
Запускаю `docker-compose.yml`:</br>
```
docker-compose up -d
```
Устанавливаю отсутствующие библиотеки. </br>
В проекте используются следующие скрипты:
-  `.env` -  файл с переменными окружения.
- `producer_pg_to_kafka.py` - скрипт, который отправляет данные в **Kafka**.
- `consumer_to_clickhouse.py` - скрипт, который читает данные из **Kafka**.
## <img width="25" height="25" alt="image" src="https://github.com/user-attachments/assets/3c0a71e8-4251-43d4-afa8-c94e32829348" /> Реализация:
***1) Создаю таблицу `user_logins` в `PostgreSQL` со следующей структурой:*** 
```
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,                -- Создан идентификатор записи
    username TEXT,                        -- Имя пользователя
    event_type TEXT,                      -- Событие, совершенное пользователем
    event_time TIMESTAMP,                 -- Время, когда было совершено событие
    sent_to_kafka BOOLEAN DEFAULT FALSE   -- Создан стобец sent_to_kafka, который по умолчанию принимает значение FALSE
)
```
- Столбец `sent_to_kafka BOOLEAN` информирует, были ли данные уже отправлены в `Kafka`.
  
***2) Добавляю тестовые данные в таблицу `user_logins`:***
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

***3) Запускаю `producer_pg_to_kafka.py` первый раз, скрипт отправляет 8 записей в `Kafka` и при этом флаг </br>
`sent_to_kafka` устанавливается для этих записей в значение **TRUE**.</br>***
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

***4) Запускаю `consumer_to_clickhouse.py` и оставляю запущенным, скрипт будет работать в режиме ожидания. </br>***
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
Консьюмер получает данные из `Kafka` и сохраняет их в `ClickHouse` в таблицу `user_logins` со следующей структурой:
```
CREATE TABLE IF NOT EXISTS user_logins (
    id UInt32,
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
```

***5) Добавляю в таблицу `user_logins` в `PostgreSQL` дополнительно 3 записи, чтобы убедиться, что продюсер не отправляет повторно записи и флаг `sent_to_kafka`  выставлен корректно:</br>***
```
insert into user_logins
(username, event_type, event_time) 
values
('bob',	'signup',	'2025-11-12 19:55:58'),
('carol',	'login',	'2025-11-12 19:55:57'),
('carol',	'purchase',	'2025-11-12 19:55:56')
```

***6) Запускаю `producer_pg_to_kafka.py` второй раз. Три записи отправляются в `Kafka`. </br>***
```
D:\DE\DE_Kafka_project\.venv\Scripts\python.exe D:\DE\DE_Kafka_project\producer_pg_to_kafka.py 
Sent: {'id': 629, 'user': 'bob', 'event': 'signup', 'timestamp': 1762977358.0}
Sent: {'id': 630, 'user': 'carol', 'event': 'login', 'timestamp': 1762977357.0}
Sent: {'id': 631, 'user': 'carol', 'event': 'purchase', 'timestamp': 1762977356.0}

Process finished with exit code 0
```

***7) Три записи получены из `Kafka`:</br>***
```
Received: {'id': 629, 'user': 'bob', 'event': 'signup', 'timestamp': 1762977358.0}
Received: {'id': 630, 'user': 'carol', 'event': 'login', 'timestamp': 1762977357.0}
Received: {'id': 631, 'user': 'carol', 'event': 'purchase', 'timestamp': 1762977356.0}
```

## <img width="23" height="25" alt="image" src="https://github.com/user-attachments/assets/506915a7-32ff-4c48-b2d8-57a86e59b354" /> Результат:
В результате проделанной работы в таблице `user_logins` в `ClickHouse` находится 11 записей. Дубликаты отсутствуют.</br>
`Id` записей могут отличаться от примера. </br>
Скрипты: `Producer_pg_to_kafka.py` и `consumer_to_clickhouse.py` - работают корректно. </br>
В результате получилось устойчивое решение миграции данных с защитой от дубликатов.
