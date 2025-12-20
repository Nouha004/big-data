'''from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, struct, to_json,
    when, avg, sum as spark_sum, count, expr, current_timestamp
)
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf

# -----------------------------
# 1. Spark session
# -----------------------------
spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# 2. UDF to convert ML Vectors to arrays
# -----------------------------
vector_to_array_udf = udf(
    lambda v: v.toArray().tolist() if v else None, 
    ArrayType(DoubleType())
)

# -----------------------------
# 3. Kafka transaction schema (NO pre-computed features)
# -----------------------------
schema = StructType([
    StructField("Transaction_ID", StringType()),
    StructField("User_ID", StringType()),
    StructField("Transaction_Amount", DoubleType()),
    StructField("Transaction_Type", StringType()),
    StructField("Timestamp", StringType()),
    StructField("Account_Balance", DoubleType()),
    StructField("Device_Type", StringType()),
    StructField("Location", StringType()),
    StructField("Merchant_Category", StringType()),
    StructField("Card_Type", StringType()),
    StructField("Card_Age", IntegerType()),
    StructField("Transaction_Distance", DoubleType()),
    StructField("Authentication_Method", StringType()),
    StructField("Risk_Score", DoubleType()),
    StructField("Is_Weekend", IntegerType())
])

# -----------------------------
# 4. Read streaming transactions from Kafka
# -----------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

transactions = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("Timestamp")) \
    .withWatermark("event_time", "10 minutes")

# -----------------------------
# 5. Use foreachBatch to compute features + predict
# -----------------------------
def process_batch(batch_df, batch_id):
    """
    Process each micro-batch:
    1. Compute 10-minute rolling features per user
    2. Join features with transactions
    3. Apply ML model
    4. Write fraud alerts to Kafka
    """
    if batch_df.isEmpty():
        return
    
    print(f"\n{'='*80}")
    print(f"Processing Batch {batch_id} - {batch_df.count()} transactions")
    print(f"{'='*80}")
    
    # Create temp view for SQL operations
    batch_df.createOrReplaceTempView("current_batch")
    
    # Compute 10-minute rolling features using SQL window
    # Features are computed for each transaction based on that user's history in this batch
    enriched = spark.sql("""
        SELECT 
            *,
            AVG(Transaction_Amount) OVER (
                PARTITION BY User_ID 
                ORDER BY CAST(event_time AS LONG)
                RANGE BETWEEN 600 PRECEDING AND CURRENT ROW
            ) as Avg_Transaction_Amount_10m,
            SUM(CASE WHEN Risk_Score >= 0.7 THEN 1 ELSE 0 END) OVER (
                PARTITION BY User_ID 
                ORDER BY CAST(event_time AS LONG)
                RANGE BETWEEN 600 PRECEDING AND CURRENT ROW
            ) as Failed_Transaction_Count_10m
        FROM current_batch
    """).fillna({
        "Avg_Transaction_Amount_10m": 0.0,
        "Failed_Transaction_Count_10m": 0
    })
    
    print(f"\nSample enriched data:")
    enriched.select(
        "Transaction_ID", "User_ID", "Transaction_Amount",
        "Avg_Transaction_Amount_10m", "Failed_Transaction_Count_10m"
    ).show(5, truncate=False)
    
    # Load ML model
    try:
        model = PipelineModel.load("fraud_detection_model")
        
        # Make predictions
        predictions = model.transform(enriched)
        
        # Extract fraud probability and flag
        fraud_decisions = predictions.withColumn(
            "prob_array", 
            vector_to_array_udf(col("probability"))
        ).withColumn(
            "fraud_flag",
            when(col("prob_array")[1] > 0.8, 1).otherwise(0)
        )
        
        # Show fraud detections
        fraud_count = fraud_decisions.filter(col("fraud_flag") == 1).count()
        print(f"\n🚨 Detected {fraud_count} potential fraud transactions in this batch")
        
        if fraud_count > 0:
            print("\nFraud transactions:")
            fraud_decisions.filter(col("fraud_flag") == 1).select(
                "Transaction_ID", "User_ID", "Transaction_Amount",
                "fraud_flag", col("prob_array")[1].alias("fraud_score")
            ).show(truncate=False)
        
        # Prepare Kafka output
        output = fraud_decisions.select(
            to_json(struct(
                col("Transaction_ID"),
                col("User_ID"),
                col("Transaction_Amount"),
                col("Transaction_Type"),
                col("event_time").cast("string").alias("event_time"),
                col("fraud_flag"),
                col("prob_array")[1].alias("fraud_score"),
                col("Avg_Transaction_Amount_10m"),
                col("Failed_Transaction_Count_10m"),
                col("Account_Balance"),
                col("Device_Type"),
                col("Location"),
                col("Merchant_Category"),
                col("Risk_Score")
            )).alias("value")
        )
        
        # Write to Kafka
        output.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "fraud_alerts") \
            .save()
        
        print(f"\n✅ Batch {batch_id} processed successfully\n")
        
    except Exception as e:
        print(f"\n❌ Error processing batch {batch_id}: {str(e)}\n")
        raise

# -----------------------------
# 6. Start streaming with foreachBatch
# -----------------------------
query = transactions.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/fraud_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

print("\n" + "="*80)
print("🚀 Real-Time Fraud Detection System Started")
print("="*80)
print("Waiting for transactions from Kafka topic 'transactions'...")
print("Fraud alerts will be sent to topic 'fraud_alerts'")
print("Press Ctrl+C to stop\n")

query.awaitTermination()'''
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