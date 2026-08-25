# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # PRODUCT CATEGORY NAME TRANSLATION
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
df_product_category = spark.read.csv("/Volumes/retail_project_b3/landing/raw_data/product_category_name_translation.csv",header =True, inferSchema = True)



# COMMAND ----------

# DBTITLE 1,Add Timestamp
df_product_category = df_product_category.withColumn("ingest_ts",current_timestamp())



# COMMAND ----------

# DBTITLE 1,Write Section
# MAGIC %md
# MAGIC ## WRITE FILE IN BRONZE **LAYER**

# COMMAND ----------

# DBTITLE 1,Write to Bronze
df_product_category.write.mode("overwrite").format("parquet").save("/Volumes/retail_project_b3/bronze/category_name_translation/")


# COMMAND ----------

