from kafka import KafkaConsumer
import json
import clickhouse_connect, os
from dotenv import load_dotenv
load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

KAFKA_HOST = os.getenv("KAFKA_HOST")

try:
    consumer = KafkaConsumer(
        "user_events",
        bootstrap_servers=KAFKA_HOST,
        auto_offset_reset='latest',
        group_id = 'group_user_events',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                                           username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

    client.command("""
    CREATE TABLE IF NOT EXISTS user_logins (
        id UInt32,
        username String,
        event_type String,
        event_time DateTime
    ) ENGINE = MergeTree()
    ORDER BY event_time
    """)

    for message in consumer:
        data = message.value
        print("Received:", data)
        client.command(
            f"INSERT INTO user_logins (id, username, event_type, event_time) VALUES "
            f"('{data['id']}', '{data['user']}', '{data['event']}', subtractHours(toDateTime({data['timestamp']}), 3))"
        )
except Exception as e:
    print(f"Возникла ошибка: {e}")
