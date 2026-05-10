from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    "lab1",
    bootstrap_servers="broker:9092",
    group_id="velocity-anomaly-detector",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest"
)

# Dla każdego user_id trzymamy kolejkę timestampów jego ostatnich transakcji
user_transactions = defaultdict(deque)

WINDOW_SECONDS = 60
LIMIT = 3

print("Nasłuchuję anomalii prędkości: > 3 transakcje tego samego user_id w 60 sekund...")

for message in consumer:
    tx = message.value

    user_id = tx.get("user_id")
    tx_id = tx.get("tx_id")
    amount = tx.get("amount")
    store = tx.get("store")
    category = tx.get("category")
    timestamp_str = tx.get("timestamp")

    if user_id is None or timestamp_str is None:
        print(f"Pominięto niepełną transakcję: {tx}")
        continue

    # Timestamp z producenta jest w formacie ISO, np. 2026-04-26T22:57:52.652119
    event_time = datetime.fromisoformat(timestamp_str)

    # Dodajemy aktualną transakcję użytkownika
    user_transactions[user_id].append(event_time)

    # Usuwamy transakcje starsze niż 60 sekund
    window_start = event_time - timedelta(seconds=WINDOW_SECONDS)

    while user_transactions[user_id] and user_transactions[user_id][0] < window_start:
        user_transactions[user_id].popleft()

    count = len(user_transactions[user_id])

    print(f"OK: {user_id} | liczba transakcji w ostatnich 60s: {count}")

    if count > LIMIT:
        print(
            f"ALERT VELOCITY: {user_id} wykonał {count} transakcji w ciągu 60 sekund | "
            f"ostatnia: {tx_id} | {amount:.2f} PLN | {store} | {category}"
        )

