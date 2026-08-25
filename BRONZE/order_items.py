# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ORDER ITEMS
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
df_order_items = spark.read.csv("/Volumes/retail_project_b3/landing/raw_data/olist_order_items_dataset.csv",header =True, inferSchema = True)


# COMMAND ----------

# DBTITLE 1,Add Timestamp
df_order_items = df_order_items.withColumn("ingest_ts",current_timestamp())


# COMMAND ----------

# DBTITLE 1,Write Section
# MAGIC %md
# MAGIC ## WRITE FILE IN BRONZE **LAYER**

# COMMAND ----------

# DBTITLE 1,Write to Bronze
df_order_items.write.mode("overwrite").format("parquet").save(f"/Volumes/retail_project_b3/bronze/order_items/")

# COMMAND ----------

