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

# Визначення нового топіку
topic_names = [
    f'{kafka_config['name']}_sensors',
    f'{kafka_config['name']}_alerts',
]

num_partitions = 2
replication_factor = 1
new_topics = [ NewTopic(name=n, num_partitions=num_partitions, replication_factor=replication_factor) for n in topic_names ]

# Створення нових топіків
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Topics are created successfully.")
    [print(topic) for topic in admin_client.list_topics() if kafka_config['name'] in topic]
except Exception as e:
    print(f"An error occurred: {e}")

# Закриття зв'язку з клієнтом
admin_client.close()
