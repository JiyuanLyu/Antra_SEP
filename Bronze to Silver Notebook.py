# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - ETL into a Silver Delta Table
# MAGIC In this notebook, I developed the Bronze to Silver Step by:
# MAGIC 
# MAGIC 1. Extract and Transform the Raw string to columns
# MAGIC 2. Load this data into Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Files in the Bronze Path

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

movies_bronze = spark.read.load(path = bronzePath).withColumn("movie", to_json("movie"))
display(movies_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze To Silver Step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write a `movie_bronze_to silver` function
# MAGIC 
# MAGIC In this part, I write a function to transform the movie table from bronze delta table to silver delta table.
# MAGIC 
# MAGIC 
# MAGIC The function will accomplish these two following steps:
# MAGIC 1. Step 1: Extract the Nested JSON from the `movie` column
# MAGIC Here I use `pyspark.sql` functions to extract the `movie` column as a new column `nested_json`
# MAGIC 2. Step 2: Create the Silver DataFrame by Unpacking the `nested_json` column
# MAGIC 
# MAGIC Note: For the transform, I rename column `ReleaseDate` to `ReleaseTime`, create a new column as `ReleaseDate` in `Date` datatype. I also create a new column called `ReleaseYear` to indicate that I'll partition later. 

# COMMAND ----------

movies_silver = movie_bronze_to_silver(movies_bronze)
display(movies_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Table for Languages
# MAGIC 
# MAGIC Here I write a function called `get_language_table()` to get a look up silver table for `Original Language`.

# COMMAND ----------

language_silver = get_language_table(movies_bronze)
display(language_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Table for genres
# MAGIC 
# MAGIC Here I write a function called `get_genres_table()` to get a look up silver table for `Original Language`.

# COMMAND ----------

genres_silver = get_genres_table(movies_bronze)
display(genres_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update movie silver table with two look-up tables

# COMMAND ----------

movies_silver.join(language_silver, movies_silver("OriginalLanguage") == language_silver("OriginalLanguage"), "right")

# COMMAND ----------

for i in range(language_silver.count()):
    print(language_silver.collect()[i][1])
#movies_silver.select("OriginalLanguage").filter(movies_silver.OriginalLanguage )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema of movie silver with an Assertion
# MAGIC 
# MAGIC Now use an assertion to check if the schema is correct for now. 

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert movies_silver.schema == _parse_datatype_string(
    """
   movie STRING,
   BackdropUrl STRING,
   Budget DOUBLE,
   CreatedBy TIMESTAMP,
   CreatedDate STRING,
   Id LONG,
   ImdbUrl STRING,
   OriginalLanguage STRING,
   Overview STRING,
   PosterUrl STRING,
   Price DOUBLE,
   ReleaseTime TIMESTAMP,
   ReleaseDate DATE,
   ReleaseYear DATE,
   Revenue DOUBLE,
   RunTime LONG,
   Tagline STRING,
   Title STRING,
   TmdbUrl STRING,
   UpdatedBy TIMESTAMP,
   UpdatedDate TIMESTAMP,
   genres STRING
"""
)
print("Assertion passed.")

# COMMAND ----------


