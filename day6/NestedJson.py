# Databricks notebook source
# MAGIC %run "/Workspace/Users/reddyharsha263@gmail.com/Hexaware training/day5/Includes"

# COMMAND ----------

df=spark.read.json(
"dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json"
,multiLine=
True
)

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

df1.createOrReplaceTempView("Adobe")

# COMMAND ----------

df1.display()
