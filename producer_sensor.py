from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_name = f'{kafka_config['name']}_sensors'

sensor_id = str(uuid.uuid4())

try:
    for i in range(3000):
    # Відправлення повідомлення в топік

        data = {
            'id': sensor_id,                        # Ідентифікатор датчика
            'timestamp': time.time(),               # Часова мітка
            'temperature': random.randint(25, 45),  # Випадкове значення температури
            'humidity': random.randint(15, 85),     # Випадкове значення вологості
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f'SND >> {topic_name}: {data}')
        time.sleep(2)

except Exception as e:
    print(f'An error occurred: {e}')
except KeyboardInterrupt as e:
    print(f'{e}. Exiting...')
finally:
    producer.close()  # Закриття producer
