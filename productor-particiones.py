from kafka import KafkaProducer
import json
import time

# Productor con solo value_serializer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

mensajes = [
    {'evento': 'login', 'usuario': 'alice'},
    {'evento': 'logout', 'usuario': 'bob'},
    {'evento': 'registro', 'usuario': 'carol'},
    {'evento': 'compra', 'usuario': 'alice'},
    {'evento': 'login', 'usuario': 'david'},
]

for mensaje in mensajes:
    producer.send('notificaciones1', value=mensaje)
    print(f"[ENVIADO] → {mensaje}")
    time.sleep(1)

producer.flush()