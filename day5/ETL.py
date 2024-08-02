# Databricks notebook source
# MAGIC %run "/Workspace/Users/reddyharsha263@gmail.com/Hexaware training/day5/Includes"

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")

# COMMAND ----------

input

# COMMAND ----------

df=spark.read.csv(f"{input}",header = True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = add_ingestion(df)

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col = ['name'
,
'country'
,
'industry'
,
'net_worth_in_billions'
,
'company'
,
'ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df2.display()

# COMMAND ----------

df3 = df2.withColumn("environment",lit(w))

# COMMAND ----------

df3.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog mhhexawaredatabricks;
# MAGIC create schema if not exists bronze;
# MAGIC use bronze;
# MAGIC

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema",True).saveAsTable("Richest_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/hexawaredatabricks/raw/output_files/HarshaVardhan/richest`
