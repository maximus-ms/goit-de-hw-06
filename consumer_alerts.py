from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id=f'{kafka_config["name"]}_group_sensor_alerts'   # Ідентифікатор групи споживачів
)

# Назва топіку
alerts_topic_name = f'{kafka_config['name']}_alerts'

# Підписка на тему
consumer.subscribe([alerts_topic_name])

print(f'Subscribed to topics "{alerts_topic_name}"')

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f'RCV << {message.value}')

except Exception as e:
    print(f'An error occurred: {e}')
except KeyboardInterrupt as e:
    print(f'{e}. Exiting...')
finally:
    # Закриття consumer
    consumer.close()

