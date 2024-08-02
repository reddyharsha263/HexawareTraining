# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/reddyharsha263@gmail.com/Hexaware training/day5/Includes"

# COMMAND ----------

df=spark.read.csv(f"{input}",header = True,)

# COMMAND ----------

from pyspark.sql.types import *
pyspark_schmema= StructType([StructField("name",StringType()),
                             StructField("country",StringType()),
                             StructField("industry",StringType()),
                             StructField("net_worth",DoubleType()),
                             StructField("company",StringType())
])

# COMMAND ----------

df1 = spark.read.schema(pyspark_schmema).csv(f"{input}",header = True,)
