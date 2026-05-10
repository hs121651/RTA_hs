# Lab 1 homework — velocity anomaly detection

Consumer reads transactions from Kafka topic `lab1`.

Alert condition:
if the same `user_id` performs more than 3 transactions within 60 seconds.

Files:
- producer.py
- consumer_velocity.py

Run producer:

python producer.py

Run consumer:

python consumer_velocity.py
