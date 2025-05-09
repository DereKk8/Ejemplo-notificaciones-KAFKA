from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

mensaje = {'evento': 'Usuario registrado', 'usuario': 'Juan'}
producer.send('notificaciones', mensaje)
producer.flush()