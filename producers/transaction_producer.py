import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer

TOPIC = "transactions"
BOOTSTRAP_SERVERS = "localhost:9092"

MERCHANT_CATEGORIES = [
    "groceries", "electronics", "travel", "fuel", "fashion", "restaurants", "pharmacy"
]

COUNTRIES = [
    ("LK", ["Colombo", "Kandy", "Galle"]),
    ("SG", ["Singapore"]),
    ("AE", ["Dubai"]),
    ("IN", ["Chennai", "Mumbai", "Delhi"]),
    ("GB", ["London"]),
    ("US", ["New York", "San Francisco"]),
]

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def make_tx(user_id: str, event_time: datetime, merchant_category: str, amount: float, country: str, city: str):
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": iso_utc(event_time),
        "merchant_category": merchant_category,
        "amount": round(amount, 2),
        "country": country,
        "city": city,
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )

    # Create a small stable set of users with "home" countries
    users = [f"u{100+i}" for i in range(30)]
    user_home = {}
    for u in users:
        country, cities = random.choice(COUNTRIES)
        user_home[u] = (country, random.choice(cities))

    print(f"[producer] Sending to topic='{TOPIC}' bootstrap='{BOOTSTRAP_SERVERS}'")
    print("[producer] Fraud injection: occasionally HIGH_AMOUNT and IMPOSSIBLE_TRAVEL")

    i = 0
    while True:
        i += 1
        now = datetime.now(timezone.utc)

        # Pick user
        user_id = random.choice(users)

        # Base (normal) transaction
        home_country, home_city = user_home[user_id]
        merchant_category = random.choice(MERCHANT_CATEGORIES)
        amount = random.uniform(5, 500)  # normal spend

        # Decide if we inject fraud
        # ~1 in 35: HIGH_AMOUNT
        # ~1 in 50: IMPOSSIBLE_TRAVEL (two events with different countries within 10 mins)
        inject_high_amount = (i % 35 == 0)
        inject_impossible_travel = (i % 50 == 0)

        if inject_high_amount:
            amount = random.uniform(6000, 12000)
            tx = make_tx(
                user_id=user_id,
                event_time=now,
                merchant_category=merchant_category,
                amount=amount,
                country=home_country,
                city=home_city,
            )
            producer.send(TOPIC, key=user_id, value=tx)
            producer.flush()
            print(f"[producer][FRAUD:HIGH_AMOUNT] user={user_id} amount={tx['amount']} {tx['country']} {tx['timestamp']}")

        elif inject_impossible_travel:
            # Event 1: home country now
            tx1 = make_tx(
                user_id=user_id,
                event_time=now,
                merchant_category=merchant_category,
                amount=random.uniform(10, 800),
                country=home_country,
                city=home_city,
            )

            # Event 2: different country within 10 minutes (e.g., +2 minutes)
            foreign_choices = [c for c in COUNTRIES if c[0] != home_country]
            foreign_country, foreign_cities = random.choice(foreign_choices)
            tx2_time = now + timedelta(minutes=2)
            tx2 = make_tx(
                user_id=user_id,
                event_time=tx2_time,
                merchant_category=random.choice(MERCHANT_CATEGORIES),
                amount=random.uniform(10, 800),
                country=foreign_country,
                city=random.choice(foreign_cities),
            )

            producer.send(TOPIC, key=user_id, value=tx1)
            producer.send(TOPIC, key=user_id, value=tx2)
            producer.flush()
            print(f"[producer][FRAUD:IMPOSSIBLE_TRAVEL] user={user_id} {tx1['country']}@{tx1['timestamp']} -> {tx2['country']}@{tx2['timestamp']}")

        else:
            # Normal transaction
            tx = make_tx(
                user_id=user_id,
                event_time=now,
                merchant_category=merchant_category,
                amount=amount,
                country=home_country,
                city=home_city,
            )
            producer.send(TOPIC, key=user_id, value=tx)

            # keep logs clean: print every 10th normal event
            if i % 10 == 0:
                print(f"[producer] normal user={user_id} amount={tx['amount']} {tx['country']} {tx['timestamp']}")

        time.sleep(1)

if __name__ == "__main__":
    main()