# Databricks notebook source
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/HarshaVardhan/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/HarshaVardhan/autoloader")
.trigger(once=True)
.table("mhhexawaredatabricks.bronze.autoloader")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mhhexawaredatabricks.bronze.autoloader

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/HarshaVardhan/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/HarshaVardhan/autoloader")
.option("mergeSchema",True)
.table("mhhexawaredatabricks.bronze.autoloader")
)

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/HarshaVardhan/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/HarshaVardhan/autoloader")
.option("mergeSchema",True)
.table("mhhexawaredatabricks.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mhhexawaredatabricks.bronze.autoloader
