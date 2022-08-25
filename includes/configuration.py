# Databricks notebook source
# MAGIC %md
# MAGIC Define data paths here

# COMMAND ----------

dataPipelinePath = f"/dbfs/FileStore/tables/sep/"
rawPath = dataPipelinePath + "raw/"
bronzePath = dataPipelinePath + "bronze/"
silverPath = dataPipelinePath + "silver/"
