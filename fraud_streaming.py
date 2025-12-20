'''from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, struct, to_json, when, udf, lit
)
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from collections import deque
from datetime import datetime, timedelta

# -----------------------------
# 1. Spark session
# -----------------------------
spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# 2. UDF to convert ML Vectors to arrays
# -----------------------------
vector_to_array_udf = udf(lambda v: v.toArray().tolist() if v else None, ArrayType(DoubleType()))

# -----------------------------
# 3. State schema for tracking user transactions
# -----------------------------
class UserState:
    def __init__(self):
        self.transactions = []  # List of (timestamp, amount, risk_score)
        
    def update(self, timestamp, amount, risk_score):
        # Keep only last 10 minutes of transactions
        cutoff = timestamp - timedelta(minutes=10)
        self.transactions = [(ts, amt, rs) for ts, amt, rs in self.transactions if ts > cutoff]
        self.transactions.append((timestamp, amount, risk_score))
        
    def get_features(self):
        if not self.transactions:
            return 0.0, 0
        avg_amount = sum(amt for _, amt, _ in self.transactions) / len(self.transactions)
        failed_count = sum(1 for _, _, rs in self.transactions if rs >= 0.7)
        return avg_amount, failed_count

# -----------------------------
# 4. Kafka transaction schema
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
# 5. Read streaming transactions from Kafka
# -----------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

transactions = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("Timestamp"))

# -----------------------------
# 6. Use foreachBatch for custom feature engineering + prediction
# -----------------------------
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # For each transaction, compute rolling features using SQL window
    from pyspark.sql.functions import avg as sql_avg, sum as sql_sum, unix_timestamp
    from pyspark.sql.window import Window
    
    # Create a window spec for each user - last 10 minutes
    window_spec = Window.partitionBy("User_ID").orderBy("event_time").rangeBetween(-600, 0)
    
    # Calculate features using batch window (this works in foreachBatch)
    with_features = batch_df.withColumn(
        "Avg_Transaction_Amount_10m",
        sql_avg("Transaction_Amount").over(window_spec)
    ).withColumn(
        "Failed_Transaction_Count_10m",
        sql_sum(when(col("Risk_Score") >= 0.7, 1).otherwise(0)).over(window_spec)
    )
    
    # Load model and make predictions
    model = PipelineModel.load("fraud_detection_model")
    predictions = model.transform(with_features)
    
    # Extract fraud decisions
    fraud_decisions = predictions.withColumn(
        "prob_array", 
        vector_to_array_udf(col("probability"))
    ).withColumn(
        "fraud_flag",
        when(col("prob_array")[1] > 0.8, 1).otherwise(0)
    )
    
    # Prepare output
    output = fraud_decisions.select(
        to_json(struct(
            col("Transaction_ID"),
            col("User_ID"),
            col("Transaction_Amount"),
            col("Transaction_Type"),
            col("event_time"),
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

# -----------------------------
# 7. Process stream using foreachBatch
# -----------------------------
query = transactions.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/fraud_checkpoint") \
    .start()

query.awaitTermination()'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, struct, to_json,
    when, avg, sum as spark_sum, unix_timestamp, udf
)
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql.window import Window

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
    .withColumn("event_time", to_timestamp("Timestamp"))

# -----------------------------
# 5. Use foreachBatch to compute features + predict
# -----------------------------
def process_batch(batch_df, batch_id):
    """
    Process each micro-batch:
    1. Compute 10-minute rolling features per user
    2. Apply ML model
    3. Write fraud alerts to Kafka
    """
    if batch_df.isEmpty():
        return
    
    print(f"\n{'='*80}")
    print(f"Processing Batch {batch_id} - {batch_df.count()} transactions")
    print(f"{'='*80}")
    
    # Convert timestamp to unix timestamp (seconds) for window functions
    batch_with_unix = batch_df.withColumn(
        "event_time_unix", 
        unix_timestamp("event_time")
    )
    
    # Create window spec - 10 minutes = 600 seconds
    # Order by unix timestamp (BIGINT) for rangeBetween to work
    window_spec = Window.partitionBy("User_ID") \
        .orderBy("event_time_unix") \
        .rangeBetween(-600, 0)  # Last 600 seconds (10 minutes)
    
    # Calculate rolling features
    enriched = batch_with_unix.withColumn(
        "Avg_Transaction_Amount_10m",
        avg("Transaction_Amount").over(window_spec)
    ).withColumn(
        "Failed_Transaction_Count_10m",
        spark_sum(when(col("Risk_Score") >= 0.7, 1).otherwise(0)).over(window_spec)
    ).fillna({
        "Avg_Transaction_Amount_10m": 0.0,
        "Failed_Transaction_Count_10m": 0
    })
    
    print(f"\nSample enriched data:")
    enriched.select(
        "Transaction_ID", "User_ID", "Transaction_Amount",
        "Avg_Transaction_Amount_10m", "Failed_Transaction_Count_10m"
    ).show(5, truncate=False)
    
    # Load ML model and make predictions
    try:
        model = PipelineModel.load("fraud_detection_model")
        predictions = model.transform(enriched)
        
        # Extract fraud probability and flag
        fraud_decisions = predictions.withColumn(
            "prob_array", 
            vector_to_array_udf(col("probability"))
        ).withColumn(
            "fraud_flag",
            when(col("prob_array")[1] > 0.8, 1).otherwise(0)
        )
        
        # Count fraud detections
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
        import traceback
        traceback.print_exc()
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

query.awaitTermination()