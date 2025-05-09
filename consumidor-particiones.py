from kafka import KafkaConsumer
import json

def seguro_deserializar(mensaje):
    try:
        texto = mensaje.decode('utf-8')
        if texto.strip() == '':
            return None
        return json.loads(texto)
    except Exception as e:
        print(f"Error deserializando: {mensaje} → {e}")
        return None

consumer = KafkaConsumer(
    'notificaciones1',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-notificaciones',
    value_deserializer=seguro_deserializar
)

print("Esperando mensajes...")

for mensaje in consumer:
    if mensaje.value is not None:
        print(f"[PARTICIÓN {mensaje.partition}] Mensaje: {mensaje.value}")