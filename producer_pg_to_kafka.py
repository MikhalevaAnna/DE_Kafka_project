import psycopg2
from kafka import KafkaProducer
import json
import time, os
from dotenv import load_dotenv
load_dotenv()

KAFKA_HOST = os.getenv("KAFKA_HOST")

POSTGRESQL_HOST = os.getenv("POSTGRESQL_HOST")
POSTGRESQL_PORT = os.getenv("POSTGRESQL_PORT")
POSTGRESQL_DATABASE = os.getenv("POSTGRESQL_DATABASE")
POSTGRESQL_USER = os.getenv("POSTGRESQL_USER")
POSTGRESQL_PASSWORD = os.getenv("POSTGRESQL_PASSWORD")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_HOST,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname=POSTGRESQL_DATABASE, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD,
    host=POSTGRESQL_HOST, port=POSTGRESQL_PORT
)
cursor = conn.cursor()

try:
    cursor.execute(
        "SELECT id, username, event_type, extract(epoch FROM event_time) as event_time, sent_to_kafka "
        "FROM user_logins WHERE sent_to_kafka = FALSE"
    )
    rows = cursor.fetchall()

    for row in rows:
        try:
            data = {
                "id":row[0],
                "user": row[1],
                "event": row[2],
                "timestamp": float(row[3])  # преобразуем Decimal → float
            }
            producer.send("user_events", value=data)
            print("Sent:", data)
            cursor.execute(
                "UPDATE user_logins "
                "SET sent_to_kafka = TRUE "
                "WHERE id = %s",
                (row[0],)
            )
            conn.commit()
            time.sleep(0.5)
        except Exception as e:
            print(f"Ошибка при отправке записи {row[0]}: {e}")
            conn.rollback()

except Exception as e:
    print(f"Возникла ошибка: {e}")

finally:
    cursor.close()
    conn.close()
    producer.close()