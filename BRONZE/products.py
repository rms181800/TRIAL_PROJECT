# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # PRODUCTS
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
df_products = spark.read.csv("/Volumes/retail_project_b3/landing/raw_data/olist_products_dataset.csv",header =True, inferSchema = True)



# COMMAND ----------

# DBTITLE 1,Add Timestamp
df_products = df_products.withColumn("ingest_ts",current_timestamp())



# COMMAND ----------

# DBTITLE 1,Write Section
# MAGIC %md
# MAGIC ## WRITE FILE IN BRONZE **LAYER**

# COMMAND ----------

df_products.write.mode("overwrite").format("parquet").save("/Volumes/retail_project_b3/bronze/products/")