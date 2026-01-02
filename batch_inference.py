import argparse
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# 1. Parse arguments
parser = argparse.ArgumentParser(description="Spark Batch Inference Job")
parser.add_argument("--input_path", required=True)
parser.add_argument("--model_path", required=True)
parser.add_argument("--output_path", required=True)

args = parser.parse_args()

# 2. Start Spark session
spark = SparkSession.builder \
    .appName("Spark Batch Inference") \
    .getOrCreate()

# 3. Read input CSV
df = spark.read.csv(
    args.input_path,
    header=True,
    inferSchema=True
)

# 4. Load pretrained model
model = PipelineModel.load(args.model_path)

# 5. Run inference
predictions = model.transform(df)

# 6. Write output as Parquet partitioned by date
predictions.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(args.output_path)

spark.stop()
