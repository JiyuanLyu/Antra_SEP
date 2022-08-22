# Databricks notebook source
# MAGIC %md
# MAGIC # Raw to Bronze Notebook
# MAGIC In this notebook, I turned 8 movie JSON files into a single bronze delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging JSON files
# MAGIC First, we need to merge all the movie JSON files into one JSON file.

# COMMAND ----------

import json

# Create a JSON file for merging
movies = {"movie": []}
for i in range(8):
    with open(dataPipelinePath + f"movie_{i}.json") as f:
        data = json.load(f)
        movies["movie"].append(data["movie"])

dbutils.fs.put(rawPath, json.dumps(movies, indent=2), True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data
# MAGIC 
# MAGIC Here we use a stream server like `Kafka` to streams files from source directory and write each line as a string to the Bronze table.

# COMMAND ----------

kafka_schema = "value STRING"

raw_movies_data_df = (
    spark.read.format("text").schema(kafka_schema).load(rawPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the raw data

# COMMAND ----------

display(raw_movies_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata
# MAGIC Now add the metadata to the raw data, here I add 4 columns:
# MAGIC - `data source`, as `antra.sep.databatch.movieshop`
# MAGIC - `status`, as `new`
# MAGIC - `ingesttime`
# MAGIC - `ingestdate`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

raw_movies_data_df = raw_movies_data_df.select(
    "value",
    lit("antra.sep.databatch.movieshop").alias("datasource"),
    lit("new").alias("status"),
    current_timestamp().alias("ingesttime"),
    current_timestamp().cast("date").alias("ingestdate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Batch to a Bronze Delta Table
# MAGIC 
# MAGIC For the bronze Delta table, I wrote it in this order:
# MAGIC (`datasource`, `ingesttime`, `value`, `status`, `p_ingestdate`)
# MAGIC 
# MAGIC Note: partition by `p_ingestdate`

# COMMAND ----------

from pyspark.sql.functions import col

(
  raw_movies_data_df.select("datasource", "ingesttime", "value", "status", col("ingestdate").alias("p_ingestdate"))
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Register the Bronze Table in the Metastore
# MAGIC 
# MAGIC I named the Bronze table as `movies_bronze`

# COMMAND ----------

# Drop the bronze table if exist
spark.sql("""
DROP TABLE IF EXISTS movies_bronze
""")

# Create the bronze table
spark.sql(f"""
CREATE TABLE movies_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Movies Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM movies_bronze

# COMMAND ----------


