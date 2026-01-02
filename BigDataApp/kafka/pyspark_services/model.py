from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, count, sum as spark_sum
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Spark session
spark = SparkSession.builder \
    .appName("FraudDetectionML") \
    .getOrCreate()

# Load dataset
df = spark.read.csv(
    "synthetic_fraud_dataset.csv",
    header=True,
    inferSchema=True
)

# Timestamp conversion
df = df.withColumn(
    "event_time",
    to_timestamp("Timestamp")
)

# Drop non-predictive columns
df = df.drop(
    "Transaction_ID",
    "User_ID",
    "Timestamp",
    "event_time"
)

# Feature definitions
categorical_cols = [
    "Transaction_Type",
    "Device_Type",
    "Location",
    "Merchant_Category",
    "Card_Type",
    "Authentication_Method"
]

numeric_cols = [
    "Transaction_Amount",
    "Account_Balance",
    # "Avg_Transaction_Amount_10m",
    # "Failed_Transaction_Count_10m",
    "Transaction_Distance",
    "Card_Age",
    "Is_Weekend",
]

# Train / test split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Categorical encoding
indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=f"{c}_idx",
        handleInvalid="keep"
    )
    for c in categorical_cols
]

encoders = [
    OneHotEncoder(
        inputCol=f"{c}_idx",
        outputCol=f"{c}_ohe"
    )
    for c in categorical_cols
]

# Feature vector
assembler = VectorAssembler(
    inputCols=numeric_cols + [f"{c}_ohe" for c in categorical_cols],
    outputCol="features"
)

# Model
lr = LogisticRegression(
    labelCol="Fraud_Label",
    featuresCol="features",
    maxIter=50
)

# Pipeline
pipeline = Pipeline(
    stages=indexers + encoders + [assembler, lr]
)

# Train model
model = pipeline.fit(train_df)

# Test model
predictions = model.transform(test_df)

# Confusion matrix
predictions.groupBy("Fraud_Label", "prediction").count().show()

# ROC-AUC
auc_eval = BinaryClassificationEvaluator(
    labelCol="Fraud_Label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = auc_eval.evaluate(predictions)
print("ROC-AUC:", auc)

# Precision & Recall
precision_eval = MulticlassClassificationEvaluator(
    labelCol="Fraud_Label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

recall_eval = MulticlassClassificationEvaluator(
    labelCol="Fraud_Label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

print("Precision:", precision_eval.evaluate(predictions))
print("Recall:", recall_eval.evaluate(predictions))

#saving model
model_path = "fraud_detection_model"
model.write().overwrite().save(model_path)
print(f"Model saved to: {model_path}")