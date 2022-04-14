# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Propagating Incremental Updates with Structured Streaming and Delta Lake
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC * Apply your knowledge of structured streaming and Auto Loader to implement a simple multi-hop architecture

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Setup
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.2L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Ingest data
# MAGIC 
# MAGIC This lab uses a collection of customer-related CSV data from DBFS found in */databricks-datasets/retail-org/customers/*.
# MAGIC 
# MAGIC Read this data using Auto Loader using its schema inference (use **`customers_checkpoint_path`** to store the schema info). Stream the raw data to a Delta table called **`bronze`**.

# COMMAND ----------

# TODO
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

query = (spark
  .readStream
  <FILL-IN>
  .load("/databricks-datasets/retail-org/customers/")
  .writeStream
  <FILL-IN>
  .table("bronze")
)

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Let's create a streaming temporary view into the bronze table, so that we can perform transforms using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Clean and enhance data
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`bronze_enhanced_temp`** that does the following:
# MAGIC * Skips records with a null **`postcode`** (set to zero)
# MAGIC * Inserts a column called **`receipt_time`** containing a current timestamp
# MAGIC * Inserts a column called **`source_file`** containing the input filename

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC SELECT
# MAGIC   <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Silver table
# MAGIC 
# MAGIC Stream the data from **`bronze_enhanced_temp`** to a table called **`silver`**.

# COMMAND ----------

# TODO
silver_checkpoint_path = f"{DA.paths.checkpoints}/silver"

query = (spark.table("bronze_enhanced_temp")
  <FILL-IN>
  .table("silver"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Let's create a streaming temporary view into the silver table, so that we can perform business-level using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Gold tables
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_temp`** that counts customers per state.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_temp AS
# MAGIC SELECT 
# MAGIC <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Finally, stream the data from the **`customer_count_temp`** view to a Delta table called **`gold_customer_count_by_state`**.

# COMMAND ----------

# TODO
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_counts"

query = (spark
  .table("customer_count_temp")
  .writeStream
  <FILL-IN>
  .table("gold_customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`gold_customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_count_by_state

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Run the following cell to remove the database and all data associated with this lab.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC By completing this lab, you should now feel comfortable:
# MAGIC * Using PySpark to configure Auto Loader for incremental data ingestion
# MAGIC * Using Spark SQL to aggregate streaming data
# MAGIC * Streaming data to a Delta table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
