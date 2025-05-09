from kafka import KafkaConsumer
import json
from kafka.errors import KafkaError

try:
    consumer = KafkaConsumer(
        'notificaciones',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Conectado al broker. Esperando mensajes...")

    for mensaje in consumer:
        print(f"Notificación recibida: {mensaje.value}")

    exit(0)

except KafkaError as e:
    print(f"Error de Kafka: {e}")

except Exception as ex:
    print(f"Ocurrió un error: {ex}")