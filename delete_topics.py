from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)


#delete all my topics
for topic in admin_client.list_topics():
    if kafka_config['name'] in topic:
        print(f'Deleting topic: {topic}')
        admin_client.delete_topics(topics=[topic])


# Закриття зв'язку з клієнтом
admin_client.close()
