# Databricks notebook source
# DBTITLE 1,CUSTOMERS BRONZE - INCREMENTAL LOAD
# MAGIC %md
# MAGIC # CUSTOMERS BRONZE - INCREMENTAL LOAD WITH AUTO LOADER
# MAGIC
# MAGIC ## CREATED BY: RAHUL M
# MAGIC ## CREATED DATE: 20260822
# MAGIC ## DESCRIPTION: Incremental ingestion from landing using Auto Loader (cloudFiles)
# MAGIC ## AUTO LOADER FEATURES:
# MAGIC - Only processes NEW files from landing
# MAGIC - Tracks processed files automatically
# MAGIC - Avoids duplicate data
# MAGIC - Schema evolution support

# COMMAND ----------

# MAGIC %md
# MAGIC ## CALLING FUNCTION NOTEBOOK

# COMMAND ----------

# MAGIC %run /Workspace/Users/rms181800@gmail.com/AZURE_B3_PROJECT_AUG/FUNCTIONS/functions

# COMMAND ----------

# MAGIC %md
# MAGIC # READ CSV FILE

# COMMAND ----------

df_customer = spark.read.csv("/Volumes/retail_project_b3/landing/raw_data/olist_customers_dataset.csv",header =True, inferSchema = True)



# COMMAND ----------

df_customer = df_customer.withColumn("ingest_ts",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE FILE IN BRONZE **LAYER**

# COMMAND ----------

# DBTITLE 1,Write to Bronze
df_customer.write.mode("overwrite").format("parquet").save(f"/Volumes/retail_project_b3/bronze/customer/")