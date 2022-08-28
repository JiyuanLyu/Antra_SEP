# Databricks notebook source
from pyspark.sql.functions import to_json, from_json, col, trunc, monotonically_increasing_id, lit
from pyspark.sql import DataFrame

# COMMAND ----------

def get_language_table(bronze:DataFrame) -> DataFrame:
    json_schema = """
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
       ReleaseDate TIMESTAMP,
       Revenue DOUBLE,
       RunTime LONG,
       Tagline STRING,
       Title STRING,
       TmdbUrl STRING,
       UpdatedBy TIMESTAMP,
       UpdatedDate TIMESTAMP,
       genres STRING
    """

    return(bronze.withColumn(
        "nested_json", from_json(col("movie"),
                                 json_schema))
          .select("movie",
                  "nested_json.*")
          .select("OriginalLanguage").distinct()
          .withColumn("Language_Id", monotonically_increasing_id()+1)
          .select("Language_Id", "OriginalLanguage")
    )

# COMMAND ----------

def movie_bronze_to_silver(bronze: DataFrame) -> DataFrame:
    
    # First of all, create a schema
    json_schema = """
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
       ReleaseDate TIMESTAMP,
       Revenue DOUBLE,
       RunTime LONG,
       Tagline STRING,
       Title STRING,
       TmdbUrl STRING,
       UpdatedBy TIMESTAMP,
       UpdatedDate TIMESTAMP,
       genres STRING
    """
    
    return(bronze.withColumn(
        "nested_json", from_json(col("movie"),
                                 json_schema))
          .select("movie", "nested_json.*")
          .select("movie",
                  "BackdropUrl",
                  "Budget",
                  "CreatedBy",
                  "CreatedDate",
                  "Id",
                  "ImdbUrl",
                  "OriginalLanguage",
                  "Overview",
                  "PosterUrl",
                  "Price",
                  col("ReleaseDate").alias("ReleaseTime"),
                  col("ReleaseDate").cast("date"),
                  trunc(col("ReleaseDate").cast("date"), "year").alias("ReleaseYear"),
                  "Revenue",
                  "RunTime",
                  "Tagline",
                  "Title",
                  "TmdbUrl",
                  "UpdatedBy",
                  "UpdatedDate",
                  "genres")
          )

# COMMAND ----------

from pyspark.sql.functions import translate, split, explode

def get_genres_pairs(bronze:DataFrame) -> DataFrame:
    json_schema = """
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
       ReleaseDate TIMESTAMP,
       Revenue DOUBLE,
       RunTime LONG,
       Tagline STRING,
       Title STRING,
       TmdbUrl STRING,
       UpdatedBy TIMESTAMP,
       UpdatedDate TIMESTAMP,
       genres STRING
    """
    
    
    
    return(bronze.withColumn(
        "nested_json", from_json(col("movie"),
                                 json_schema))
          .select("movie",
                  "nested_json.*")
          .select("genres")
    )

# COMMAND ----------

def get_genres_table(g1: DataFrame) -> DataFrame:
    g1 = get_genres_pairs(movies_bronze).withColumn("genres",translate("genres","[]","")).select(split(col("genres"), "},")
                                        .alias("genresArray")).drop("genres").select("genresArray", explode("genresArray")).drop("genresArray").withColumn("col",translate("col","{\"}","")).distinct().select(split(col("col"), ","))
    genres_id = []
    name = []
    for i in range(g1.count()):
        pair = g1.collect()[i][0]
        genres_id.append(pair[0][3:])
        name.append(pair[1][5:])
    genres_silver = spark.createDataFrame(zip(genres_id, name), ["genres_Id", "name"])
    genres_silver = genres_silver.withColumn("genres_Id", col("genres_Id").cast("integer")).sort("genres_Id")
    
    return(genres_silver)

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, udf, explode, regexp_replace

zip_ = udf(lambda x, y: list(zip(x, i) for i in y),
           ArrayType(StructType([
               StructField("first", IntegerType()),
               StructField("second", StringType())
           ]))
          )

# COMMAND ----------

def get_movie_genre_junction_table(movies_silver: DataFrame) -> DataFrame:
    test_ms = movies_silver
    #test_ms = test_ms.withColumn("movie_genre_junction_id", monotonically_increasing_id()+1)

    mgj_1 = test_ms.select("movie_genre_junction_id", "genres", split(col("genres"), "},")).withColumn("genre", col("split(genres, },, -1)")).drop("genres", "split(genres, },, -1)")
    mgj_2 = mgj_1.select("movie_genre_junction_id", "genre").withColumn("genre", explode("genre")).withColumn("genreId", regexp_replace("genre","\\D", "")).drop("genre")
    
    return(mgj_2)

# COMMAND ----------

def set_df_columns_nullable(spark, df, column_list, nullable=True):
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod
