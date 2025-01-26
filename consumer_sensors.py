import os

from configs import kafka_config
import time

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from configs import kafka_config


# Назва топіків
sensors_topic_name = f'{kafka_config['name']}_sensors'
alerts_topic_name = f'{kafka_config['name']}_alerts'


# Пакет, необхідний для читання Kafka зі Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


# Створення SparkSession
spark = (SparkSession.builder
            .appName('KafkaStreaming')
            .master('local[*]')
            .getOrCreate())


# Завантаження параметрів з файлу
alerts_conditions_file = './alerts_conditions.csv'
cond_df = spark.read.csv(alerts_conditions_file, header=True) \
                        .withColumnRenamed('humidity_min', 'h_min') \
                        .withColumnRenamed('humidity_max', 'h_max') \
                        .withColumnRenamed('temperature_min', 't_min') \
                        .withColumnRenamed('temperature_max', 't_max') \
                        .drop('id')

print('Alerts conditions loaded:')
cond_df.show()


# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON.
json_schema = StructType([
    StructField('id', StringType(), False),
    StructField('timestamp', StringType(), True),
    StructField('temperature', IntegerType(), True),
    StructField('humidity', IntegerType(), True)
])


try:
    # Читання потоку даних із Kafka
    # Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
    # maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
            # .option('kafka.group_id', f'{kafka_config["name"]}_group_sensor_processor') \
    df = spark \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_config['bootstrap_servers'][0]) \
            .option('kafka.security.protocol', 'SASL_PLAINTEXT') \
            .option('kafka.sasl.mechanism', 'PLAIN') \
            .option('kafka.sasl.jaas.config',
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config['username']}" password="{kafka_config['password']}";') \
            .option('subscribe', sensors_topic_name) \
            .option('startingOffsets', 'earliest') \
            .option('maxOffsetsPerTrigger', '5') \
            .load()

    # Маніпуляції з даними
    clean_df = df.selectExpr('CAST(key AS STRING) AS key_deserialized', 'CAST(value AS STRING) AS value_deserialized', '*') \
        .drop('key', 'value') \
        .withColumnRenamed('key_deserialized', 'key') \
        .withColumn('value_json', from_json(col('value_deserialized'), json_schema)) \
        .withColumn('timestamp', from_unixtime(col('value_json.timestamp').cast(DoubleType())).cast('timestamp')) \
        .withColumn('id', col('value_json.id')) \
        .withColumn('temperature', col('value_json.temperature')) \
        .withColumn('humidity', col('value_json.humidity')) \
        .drop('value_json', 'value_deserialized')

    # Застосування ковзного вікна і вотермарку
    windowed_df = clean_df \
        .withWatermark('timestamp', '10 seconds') \
        .groupBy(window(col('timestamp'), '1 minute', '30 seconds'),) \
        .agg(
            round(avg('temperature'), 2).alias('t_avg'),
            round(avg('humidity'), 2).alias('h_avg')
        ) \
        .withColumn('timestamp', lit(time.time()).cast('timestamp'))

    filtered_df = windowed_df \
        .join(cond_df, \
                (((windowed_df['t_avg'] > cond_df['t_min']) & (windowed_df['t_avg'] < cond_df['t_max'])) \
                |((windowed_df['h_avg'] > cond_df['h_min']) & (windowed_df['h_avg'] < cond_df['h_max']))) \
            , 'inner') \
        .drop('t_min', 't_max', 'h_min', 'h_max')

    # Підготовка даних для запису в Kafka: формування ключ-значення
    prepare_to_kafka_df = filtered_df.select(
        to_json(
            struct(
                struct(
                    col('window.start').alias('start'),
                    col('window.end').alias('end'),
                ).alias('window'),
                col('t_avg'),
                col('h_avg'),
                col('code'),
                col('message'),
                col('timestamp')
            )
        ).alias('value')
    )

    # displaying_df = prepare_to_kafka_df.writeStream \
    #     .trigger(availableNow=True) \
    #     .outputMode('append') \
    #     .format('console') \
    #     .option('checkpointLocation', f'/tmp/checkpoints-{alerts_topic_name}-display') \
    #     .start() \
    #     .awaitTermination()

    # Запис оброблених даних у Kafka-топік 'alerts_topic_name'
    query = prepare_to_kafka_df.writeStream \
        .trigger(processingTime='5 seconds') \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_config['bootstrap_servers'][0]) \
        .option('topic', alerts_topic_name) \
        .option('kafka.security.protocol', 'SASL_PLAINTEXT') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config',
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config['username']}" password="{kafka_config['password']}";') \
        .option('checkpointLocation', f'/tmp/checkpoints-{alerts_topic_name}') \
        .start() \
        .awaitTermination()


except Exception as e:
    print(f'An error occurred: {e}')
except KeyboardInterrupt as e:
    print(f'{e}. Exiting...')
finally:
    spark.stop()
