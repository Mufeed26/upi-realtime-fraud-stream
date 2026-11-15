import random
import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime

DB_NAME = "upi_stream.db"

# Each user has a realistic home city (used 85% of the time to prevent false positives)
USER_PROFILES = {
    "arjun@okaxis": "Bengaluru",
    "priya@okicici": "Mumbai",
    "rohan@oksbi": "Delhi",
    "neha@okhdfc": "Hyderabad",
    "vikram@ybl": "Chennai",
}
ALL_CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai", "Pune"]
MERCHANTS = ["Swiggy", "Amazon", "Zomato", "IRCTC", "Reliance Digital", "Unknown_QR_99"]


def setup_database():
    """Creates dual sinks: clean_transactions for valid events, fraud_alerts for blocked events."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clean_transactions (
            txn_id TEXT PRIMARY KEY,
            upi_id TEXT,
            amount_inr REAL,
            city TEXT,
            merchant TEXT,
            event_time TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            txn_id TEXT PRIMARY KEY,
            upi_id TEXT,
            amount_inr REAL,
            city TEXT,
            merchant TEXT,
            fraud_reason TEXT,
            event_time TEXT
        )
    """)

    conn.commit()
    conn.close()


def generate_live_event(txn_counter):
    """Simulates live UPI payments with ~85% normal behavior and ~15% anomaly patterns."""
    upi_id = random.choice(list(USER_PROFILES.keys()))
    home_city = USER_PROFILES[upi_id]
    merchant = random.choice(MERCHANTS)

    if random.random() < 0.85:
        city = home_city
        amount = round(random.uniform(50, 4500), 2)
        hour = random.randint(8, 22)
    else:
        # Simulate anomalous behavior: foreign city jump, late-night spike, or high amount
        city = random.choice([c for c in ALL_CITIES if c != home_city])
        amount = round(random.uniform(35000, 95000), 2)
        hour = random.choice([2, 3, 14, 23])

    event_time = datetime.now().replace(hour=hour).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "txn_id": f"TXN{100000 + txn_counter}",
        "upi_id": upi_id,
        "amount_inr": amount,
        "city": city,
        "merchant": merchant,
        "hour": hour,
        "timestamp_sec": time.time(),
        "event_time": event_time,
    }


class FraudStreamProcessor:
    """Stateful stream processor tracking sliding user activity windows in memory."""

    def __init__(self):
        self.user_history = defaultdict(lambda: deque(maxlen=5))

    def evaluate(self, event):
        reasons = []
        upi = event["upi_id"]
        recent_txns = self.user_history[upi]

        # Rule 1: High-value transfer during suspicious late-night hours (1 AM - 4 AM)
        if event["amount_inr"] > 50000 and 1 <= event["hour"] <= 4:
            reasons.append("HIGH_VALUE_LATE_NIGHT")

        # Rule 2: Impossible travel (different city within 12 seconds in stream)
        if recent_txns:
            last_txn = recent_txns[-1]
            time_diff = event["timestamp_sec"] - last_txn["timestamp_sec"]
            if last_txn["city"] != event["city"] and time_diff < 12:
                reasons.append(f"IMPOSSIBLE_TRAVEL ({last_txn['city']}->{event['city']})")

        # Rule 3: Rapid velocity burst (3+ transactions within 6 seconds)
        recent_burst = [t for t in recent_txns if (event["timestamp_sec"] - t["timestamp_sec"]) <= 6]
        if len(recent_burst) >= 2:
            reasons.append("VELOCITY_BURST_3+_TXNS")

        recent_txns.append(event)
        return reasons


def run_stream():
    setup_database()
    processor = FraudStreamProcessor()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Clear old rows for a fresh demo run
    cursor.execute("DELETE FROM clean_transactions")
    cursor.execute("DELETE FROM fraud_alerts")
    conn.commit()

    print("LIVE UPI STREAM STARTED (Press Ctrl + C to stop)\n")
    print(f"{'TXN ID':<10} | {'UPI ID':<16} | {'AMOUNT':<11} | {'CITY':<10} | {'STATUS'}")
    print("-" * 78)

    txn_counter = 1
    try:
        while True:
            event = generate_live_event(txn_counter)
            fraud_reasons = processor.evaluate(event)

            if fraud_reasons:
                reason_str = " + ".join(fraud_reasons)
                cursor.execute(
                    "INSERT OR IGNORE INTO fraud_alerts VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event["txn_id"],
                        event["upi_id"],
                        event["amount_inr"],
                        event["city"],
                        event["merchant"],
                        reason_str,
                        event["event_time"],
                    ),
                )
                print(
                    f"\033[91m{event['txn_id']:<10} | {event['upi_id']:<16} | "
                    f"₹{event['amount_inr']:<9.2f} | {event['city']:<10} | "
                    f"FRAUD ALERT: {reason_str}\033[0m"
                )
            else:
                cursor.execute(
                    "INSERT OR IGNORE INTO clean_transactions VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event["txn_id"],
                        event["upi_id"],
                        event["amount_inr"],
                        event["city"],
                        event["merchant"],
                        event["event_time"],
                    ),
                )
                print(
                    f"\033[92m{event['txn_id']:<10} | {event['upi_id']:<16} | "
                    f"₹{event['amount_inr']:<9.2f} | {event['city']:<10} | "
                    f"CLEAN\033[0m"
                )

            conn.commit()
            txn_counter += 1
            time.sleep(0.4)

    except KeyboardInterrupt:
        clean_count = cursor.execute("SELECT COUNT(*) FROM clean_transactions").fetchone()[0]
        fraud_count = cursor.execute("SELECT COUNT(*) FROM fraud_alerts").fetchone()[0]
        conn.close()
        print("\n" + "=" * 78)
        print(f"STREAM STOPPED | Clean Transactions: {clean_count} | Fraud Alerts Caught: {fraud_count}")


if __name__ == "__main__":
    run_stream()