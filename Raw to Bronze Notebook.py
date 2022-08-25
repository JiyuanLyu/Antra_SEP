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

from pyspark.sql.functions import explode, col
movie_raw = spark.read.json(path = f"/FileStore/tables/sep/*", multiLine = True)
movie_raw = movie_raw.select("movie", explode("movie"))
movie_raw = movie_raw.drop(col("movie")).toDF('movie')

display(movie_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the raw data

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

movie_raw = movie_raw.select(
    "movie",
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

(
  movie_raw.select("datasource", "ingesttime", "movie", "status", col("ingestdate").alias("p_ingestdate"))
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

display(movie_raw)

# COMMAND ----------


