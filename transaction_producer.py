import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Categorical values
transaction_types = ['POS', 'Bank Transfer', 'Online', 'ATM Withdrawal']
device_types = ['Laptop', 'Mobile', 'Tablet']
locations = ['Sydney', 'New York', 'Mumbai', 'Tokyo', 'London']
merchant_categories = ['Travel', 'Clothing', 'Restaurants', 'Electronics', 'Groceries']
card_types = ['Amex', 'Mastercard', 'Visa', 'Discover']
auth_methods = ['Biometric', 'Password', 'OTP', 'PIN']

print("="*80)
print("🚀 Transaction Generator Started")
print("="*80)
print("Streaming transactions to Kafka topic 'transactions'...")
print("Press Ctrl+C to stop\n")

transaction_count = 0

try:
    while True:
        now = datetime.now()
        
        # Build transaction (NO pre-computed features)
        transaction = {
            "Transaction_ID": f"TXN_{random.randint(10000, 99999)}",
            "User_ID": f"USER_{random.randint(1000, 9999)}",
            "Transaction_Amount": round(random.uniform(1.0, 1200.0), 2),
            "Transaction_Type": random.choice(transaction_types),
            "Timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
            "Account_Balance": round(random.uniform(500.0, 100000.0), 2),
            "Device_Type": random.choice(device_types),
            "Location": random.choice(locations),
            "Merchant_Category": random.choice(merchant_categories),
            "Card_Type": random.choice(card_types),
            "Card_Age": random.randint(1, 300),
            "Transaction_Distance": round(random.uniform(0.1, 5000.0), 2),
            "Authentication_Method": random.choice(auth_methods),
            "Risk_Score": round(random.uniform(0.0, 1.0), 4),
            "Is_Weekend": 1 if now.weekday() >= 5 else 0
        }
        
        # Send to Kafka
        producer.send("transactions", transaction)
        transaction_count += 1
        
        # Print with color
        print(f"✅ [{transaction_count:4d}] Sent: {transaction['Transaction_ID']} | "
              f"{transaction['User_ID']} | {transaction['Location']:10s} | "
              f"${transaction['Transaction_Amount']:7.2f} | "
              f"Risk: {transaction['Risk_Score']:.2f}")
        
        # Simulate real-time traffic
        time.sleep(random.uniform(0.5, 2.0))
        
except KeyboardInterrupt:
    print("\n" + "="*80)
    print(f"Stopping transaction generator... Sent {transaction_count} transactions")
    print("="*80)
finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed successfully")
