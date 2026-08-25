# Databricks notebook source
# DBTITLE 1,SELLERS BRONZE - INCREMENTAL LOAD
# MAGIC %md
# MAGIC # SELLERS BRONZE - INCREMENTAL LOAD WITH AUTO LOADER
# MAGIC
# MAGIC ## CREATED BY: RAHUL M
# MAGIC ## CREATED DATE: 20260822
# MAGIC ## DESCRIPTION: 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Calling Function


# COMMAND ----------

# DBTITLE 1,Run Functions
# MAGIC %run /Workspace/Users/rms181800@gmail.com/AZURE_B3_PROJECT_AUG/FUNCTIONS/functions

# COMMAND ----------

# DBTITLE 1,Read Section
# MAGIC %md
# MAGIC # READ CSV FILE

# COMMAND ----------

# DBTITLE 1,Read CSV - Sellers

df_sellers = spark.read.csv("/Volumes/retail_project_b3/landing/raw_data/olist_sellers_dataset.csv", header=True, inferSchema=True)


# COMMAND ----------

# DBTITLE 1,Add Audit Timestamp
# Add audit timestamp
df_sellers_with_ts = df_sellers.withColumn("ingest_ts", current_timestamp())

print("✅ Audit timestamp added")

# COMMAND ----------

# DBTITLE 1,Write Section
# MAGIC %md
# MAGIC ## WRITE FILE IN BRONZE **LAYER**

# COMMAND ----------

# DBTITLE 1,Write to Bronze - Full Load
df_sellers_with_ts.write.mode("overwrite").format("parquet").save("/Volumes/retail_project_b3/bronze/sellers/")
