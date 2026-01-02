from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

# 1. Start Spark session
spark = SparkSession.builder \
    .appName("Train Spark ML Pipeline") \
    .getOrCreate()

# 2. Read cleaned CSV data
df = spark.read.csv(
    "data/input/input_data_clean.csv",
    header=True,
    inferSchema=True
)

# 3. Define label and feature columns
label_col = "label"

feature_cols = [
    c for c in df.columns
    if c not in ["ID", label_col, "event_date"]
]

# 4. Assemble features into a vector
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw"
)

# 5. Scale features (best practice)
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features"
)

# 6. Classifier
lr = LogisticRegression(
    featuresCol="features",
    labelCol=label_col,
    maxIter=20
)

# 7. Build pipeline
pipeline = Pipeline(stages=[
    assembler,
    scaler,
    lr
])

# 8. Train pipeline
model = pipeline.fit(df)

# 9. Save trained model
model.write().overwrite().save("model/spark_ml_model")

spark.stop()
