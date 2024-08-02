# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists mhhexawaredatabricks.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table mhhexawaredatabricks.silver.richest_silver as
# MAGIC select name, country, industry, net_worth_in_billions, company from mhhexawaredatabricks.bronze.richest_bronze 

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists mhhexawaredatabricks.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.country_count as  
# MAGIC select country, count(country) as count from mhhexawaredatabricks.silver.richest_silver group by country order by count desc
