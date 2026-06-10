from pyspark.sql import SparkSession
from faker import Faker
import pandas as pd
import random
import numpy as np

spark = SparkSession.builder.appName("FakeData").getOrCreate()
fake = Faker()

def generate_data(num_records):
    data = []
    for _ in range(num_records):
        data.append({
            "transaction_id": fake.uuid4(),
            "customer_id": random.randint(1000, 9999),
            "name": fake.name(),
            "email": fake.email(),
            "amount": np.random.uniform(1.0, 10000.0),
            "timestamp": fake.date_time_this_year(),
            "location": fake.country(),
            "is_fraud": random.choices([0, 1], weights=[0.95, 0.05])[0] # 5% fraud
        })
    return pd.DataFrame(data)

# Create 10,000 records
pdf = generate_data(10000)
df = spark.createDataFrame(pdf)
df.show()





from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col, when

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("RiskControl").getOrCreate()

# 2. Load Sample Data (e.g., credit transactions)
data = spark.read.csv("loan_data.csv", header=True, inferSchema=True)

# 3. Data Preprocessing & Feature Engineering
# Create a binary target "label": 1 if risk_score < 600, else 0
data = data.withColumn("label", when(col("credit_score") < 600, 1).otherwise(0))

# Convert categorical variables
indexer = StringIndexer(inputCol="purpose", outputCol="purpose_index")
data = indexer.fit(data).transform(data)

# Combine features into a single vector
feature_cols = ["amount", "duration", "purpose_index", "income"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
final_data = assembler.transform(data).select("features", "label")

# 4. Train/Test Split
train_df, test_df = final_data.randomSplit([0.7, 0.3], seed=42)

# 5. Model Training (Logistic Regression)
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
lr_model = lr.fit(train_df)

# 6. Evaluation
predictions = lr_model.transform(test_df)
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label")
auc = evaluator.evaluate(predictions)
print(f"Area Under ROC Curve: {auc}")

# Show predictions
predictions.select("features", "label", "prediction", "probability").show(5)


