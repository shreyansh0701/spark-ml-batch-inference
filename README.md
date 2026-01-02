# Spark Batch Inference Pipeline

## Overview
This project implements a batch inference pipeline using Apache Spark ML.
A pretrained Spark ML PipelineModel is loaded to perform classification
inference on a CSV dataset, and predictions are written to Parquet format
partitioned by date.

## Dataset
UCI Credit Card Default dataset (converted from Excel to CSV).
A preprocessing step was applied to clean headers and add an event_date column.

## Pipeline
- CSV ingestion using Spark DataFrames
- Feature assembly and scaling using Spark ML
- Logistic Regression classifier
- Batch inference using pretrained PipelineModel
- Output written as partitioned Parquet files

### Execution Environment

Apache Spark runs most reliably on Linux systems. Running Spark directly on
Windows or macOS can lead to filesystem and Hadoop native library issues.

To avoid OS-specific problems and ensure consistent execution, this project
runs Spark inside a Dockerized Linux environment using the official Apache
Spark image.

This approach provides a clean, reproducible setup and reflects how Spark
batch jobs are commonly executed in real-world environments.

---

### Docker Setup

Build the Docker image from the project root:

```
docker build -t spark-ml .
```
Run the Spark container while mounting the project directory:

```
docker run -it -v "$(pwd):/app" spark-ml
```
This starts a Linux-based Spark environment with access to the project files.

Train the Model
Inside the Docker container, run:

```
/opt/spark/bin/spark-submit train_model.py
```
This step:

reads the cleaned CSV dataset

trains a Spark ML pipeline

saves the pretrained model to:

```
model/spark_ml_model/
```
Run Batch Inference
After training completes, run:

```
/opt/spark/bin/spark-submit batch_inference.py \
  --input_path data/input/input_data_clean.csv \
  --model_path model/spark_ml_model \
  --output_path data/output/predictions
```
This step:

loads the pretrained model

performs batch inference on the input data

writes predictions in Parquet format

partitions output by the event_date column

Output
After successful execution, the following directories are created:

Trained model:

```
model/spark_ml_model/
```
Batch inference output:

```
data/output/predictions/event_date=YYYY-MM-DD/
```