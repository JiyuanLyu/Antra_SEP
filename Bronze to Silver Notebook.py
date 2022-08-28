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

movies_bronze = spark.read.load(path = bronzePath)#.withColumn("movie", to_json("movie"))
display(movies_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze To Silver Step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write a `movie_bronze_to silver` function
# MAGIC 
# MAGIC In this part, I write a function to transform the movie table from bronze delta table to silver delta table.
# MAGIC Details in `includes/main/python/operation`
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
# MAGIC Here I write a function called `get_language_table()` to get a look up silver table for `Original Language`. Details in `includes/main/python/operation`

# COMMAND ----------

language_silver = get_language_table(movies_bronze)
display(language_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Table for genres
# MAGIC 
# MAGIC Here I write two function to get a look up silver table for `Original Language`. Details in `includes/main/python/operation`

# COMMAND ----------

genres_silver = get_genres_table(movies_bronze)
display(genres_silver)

# COMMAND ----------

movies_silver = movies_silver.withColumn("movie_genre_junction_id", monotonically_increasing_id()+1)
movie_genre_junction_silver = get_movie_genre_junction_table(movies_silver)
display(movie_genre_junction_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update movie silver table with two look-up tables
# MAGIC 
# MAGIC 
# MAGIC Two problem need to be solve:
# MAGIC - for the language column in movie table, need to be replaced by the language_id from language look-up table
# MAGIC - for the genres column in movie table, delete all the char but not the numbers, the ideal datatype for this column should be array holding integers

# COMMAND ----------

# Update with language silver table
movies_silver = movies_silver.join(language_silver, movies_silver.OriginalLanguage == language_silver.OriginalLanguage, "inner").drop("OriginalLanguage")
display(movies_silver)

# COMMAND ----------

# Update with movies-genres junction table
movies_silver = movies_silver.drop("genres")
display(movies_silver)

# COMMAND ----------

set_df_columns_nullable(spark, movies_silver,['Language_Id','movie_genre_junction_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema of movie silver with an Assertion
# MAGIC 
# MAGIC Now use an assertion to check if the schema is correct for now. 
# MAGIC 
# MAGIC Note: Need to replace the look-up table variables here.

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
    movie_genre_junction_id long,
    Language_Id long
    """
)
print("Assertion passed.")

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert movies_silver.schema == _parse_datatype_string(
    """
    movie string,
    BackdropUrl string,
    Budget double,
    CreatedBy timestamp,
    CreatedDate string,
    Id long,
    ImdbUrl string,
    Overview string,
    PosterUrl string,
    Price double,
    ReleaseTime timestamp,
    ReleaseDate date,
    ReleaseYear date,
    Revenue double,
    RunTime long,
    Tagline string,
    Title string,
    TmdbUrl string,
    UpdatedBy timestamp,
    UpdatedDate timestamp,
    movie_genre_junction_id long,
    Language_Id long
    """
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Quarantine the Bad Data
# MAGIC 
# MAGIC 
# MAGIC Some movies have a negative `runtime` value, therefore mark the negative value as `quarantined` and split the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split the silver dataframe

# COMMAND ----------

movies_silver_clean = movies_silver.filter(movies_silver.RunTime >= 0)
movies_silver_quarantined = movies_silver.filter(movies_silver.RunTime < 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Quarantined Records

# COMMAND ----------

display(movies_silver_quarantined)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC %md
# MAGIC ## WRITE Clean Batch to a Silver Table
# MAGIC 
# MAGIC Here I write `movies_silver_clean` to the Silver table path, `silverPath`.
# MAGIC 
# MAGIC Note:
# MAGIC 1. Use format, `"delta"`
# MAGIC 1. Use mode `"append"`.
# MAGIC 1. Do **NOT** include the `movie` column.
# MAGIC 1. Partition by `"ReleaseYear"`.

# COMMAND ----------

(
    movies_silver_clean.select(
        "BackdropUrl", "Budget", "CreatedBy", "CreatedDate",
        "Id", "ImdbUrl", "Overview", "PosterUrl", "Price", "ReleaseTime", "ReleaseDate",
        "ReleaseYear", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy",
        "UpdatedDate", "movie_genre_junction_id", "Language_Id"
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("ReleaseYear")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movies_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

silverTable = spark.read.table("movies_silver")
expected_schema = """
    BackdropUrl string,
    Budget double,
    CreatedBy timestamp,
    CreatedDate string,
    Id long,
    ImdbUrl string,
    Overview string,
    PosterUrl string,
    Price double,
    ReleaseTime timestamp,
    ReleaseDate date,
    ReleaseYear date,
    Revenue double,
    RunTime long,
    Tagline string,
    Title string,
    TmdbUrl string,
    UpdatedBy timestamp,
    UpdatedDate timestamp,
    movie_genre_junction_id long,
    Language_Id long
"""

assert silverTable.schema == _parse_datatype_string(
    expected_schema
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies_silver

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Bronze table to Reflect the Loads

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Clean Records
# MAGIC 
# MAGIC Here I'll use the `movie` column in the `movies_silver_clean` DataFrame to match the `movie` column in the `movies_bronze` table.

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = movies_silver_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set = update)
    .execute()
)

# COMMAND ----------

movies_bronze = spark.read.load(path = bronzePath)
movies_bronze.printSchema()

silverAugmented.printSchema()

# COMMAND ----------

from pyspark.sql import DataFrame
movies_silver_clean.printSchema()
silverAugmented.printSchema()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Quarantined Records
# MAGIC 
# MAGIC The quarantined records will be marked as `"quarantined"` in the `status` column.
# MAGIC 
# MAGIC Same as clean record,  I'll use the `movie` column in the `movies_silver_clean` DataFrame to match the `movie` column in the `movies_bronze` table.

# COMMAND ----------

silverAugmented = movies_silver_quarantined.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------



# COMMAND ----------


