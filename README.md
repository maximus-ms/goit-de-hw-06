## Домашнє завдання до теми «Spark streaming»

#### 1. Генерація потоку даних

Генерацію імплементовано в файлі `producer_sensor.py`.
Каожне повідомлення відправляється в топік `f'{kafka_config['name']}_sensors'`.

Формат повідомлення

![sensor_format](screenshots/p4.png)

Запускаємо 4 потоки:

![p1](screenshots/p1.png)

#### 2. Обробка даних

Читання, агрегація, фільтрація та відправка даних в топік `f'{kafka_config['name']}_alerts'` виконанна в файлі `consumer_sensors.py`.

Можемо бачити повідомлення від сенсорів в топіку `*_sensors'` та відфільтровані повідомлення в топіку `*_alerts'`.

![p3](screenshots/p3.png)

#### 3. Отримання повідомлень

Формат повідомлення

![alert_format](screenshots/p5.png)

Читання повідомлень виконанно в файлі `consumer_alerts.py`.

![p2](screenshots/p2.png)
