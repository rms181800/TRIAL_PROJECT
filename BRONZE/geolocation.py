# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # GEOLOCATION
# MAGIC
# MAGIC ## CREATED BY   :-    PRAVIN ABCD
# MAGIC ## CREATED DATE :-    20260820
# MAGIC ## DESCRIPTION  :-
# MAGIC ## MODIFIED BY  :-
# MAGIC ## MODIFICATION DATE :-

# COMMAND ----------

# DBTITLE 1,Calling Function
# MAGIC %md
# MAGIC ## CALLING FUNCTION NOTEBOOK

# COMMAND ----------

# DBTITLE 1,Run Functions
# MAGIC %run /Workspace/Users/rms181800@gmail.com/AZURE_B3_PROJECT_AUG/FUNCTIONS/functions

# COMMAND ----------

# DBTITLE 1,Read Section
# MAGIC %md
# MAGIC # READ CSV FILE

# COMMAND ----------

# DBTITLE 1,Read CSV
df_geolocation = spark.read.csv("/Volumes/retail_project_b3/landing/raw_data/olist_geolocation_dataset.csv",header =True, inferSchema = True)



# COMMAND ----------

# DBTITLE 1,Add Timestamp
df_geolocation = df_geolocation.withColumn("ingest_ts",current_timestamp())



# COMMAND ----------

# DBTITLE 1,Write Section
# MAGIC %md
# MAGIC ## WRITE FILE IN BRONZE **LAYER**

# COMMAND ----------

# DBTITLE 1,Write to Bronze
df_geolocation.write.mode("overwrite").format("parquet").save(f"/Volumes/retail_project_b3/bronze/geolocation/")

# COMMAND ----------

