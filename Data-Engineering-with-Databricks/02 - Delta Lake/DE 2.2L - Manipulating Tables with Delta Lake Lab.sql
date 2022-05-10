-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC # Manipulating Tables with Delta Lake
-- MAGIC 
-- MAGIC This notebook provides a hands-on review of some of the basic functionality of Delta Lake.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Execute standard operations to create and manipulate Delta Lake tables, including:
-- MAGIC   - **`CREATE TABLE`**
-- MAGIC   - **`INSERT INTO`**
-- MAGIC   - **`SELECT FROM`**
-- MAGIC   - **`UPDATE`**
-- MAGIC   - **`DELETE`**
-- MAGIC   - **`MERGE`**
-- MAGIC   - **`DROP TABLE`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.2L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create a Table
-- MAGIC 
-- MAGIC In this notebook, we'll be creating a table to track our bean collection.
-- MAGIC 
-- MAGIC Use the cell below to create a managed Delta Lake table named **`beans`**.
-- MAGIC 
-- MAGIC Provide the following schema:
-- MAGIC 
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS beans
  (name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return as error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Insert Data
-- MAGIC 
-- MAGIC Run the following cell to insert three rows into the table.

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Insert the additional records provided below. Make sure you execute this as a single transaction.

-- COMMAND ----------

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the data is in the proper state.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Update Records
-- MAGIC 
-- MAGIC A friend is reviewing your inventory of beans. After much debate, you agree that jelly beans are delicious.
-- MAGIC 
-- MAGIC Run the following cell to update this record.

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC You realize that you've accidentally entered the weight of your pinto beans incorrectly.
-- MAGIC 
-- MAGIC Update the **`grams`** column for this record to the correct weight of 1500.

-- COMMAND ----------

UPDATE beans
SET grams = 1500
WHERE name = "pinto"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm this has completed properly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
-- MAGIC row = spark.table("beans").filter("name='pinto'").first()
-- MAGIC assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
-- MAGIC assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
-- MAGIC assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Delete Records
-- MAGIC 
-- MAGIC You've decided that you only want to keep track of delicious beans.
-- MAGIC 
-- MAGIC Execute a query to drop all beans that are not delicious.

-- COMMAND ----------

DELETE FROM beans
WHERE delicious = false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to confirm this operation was successful.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Using Merge to Upsert Records
-- MAGIC 
-- MAGIC Your friend gives you some new beans. The cell below registers these as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC In the cell below, use the above view to write a merge statement to update and insert new records to your **`beans`** table as one transaction.
-- MAGIC 
-- MAGIC Make sure your logic:
-- MAGIC - Matches beans by name **and** color
-- MAGIC - Updates existing beans by adding the new weight to the existing weight
-- MAGIC - Inserts new beans only if they are delicious

-- COMMAND ----------

MERGE INTO beans b
USING new_beans u
ON b.name=u.name AND b.color = u.color
WHEN MATCHED
  THEN UPDATE SET grams = u.grams
WHEN NOT MATCHED AND u.delicious = true
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC version = spark.sql("DESCRIBE HISTORY beans").selectExpr("max(version)").first()[0]
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").filter(f"version={version}")
-- MAGIC assert last_tx.select("operation").first()[0] == "MERGE", "Transaction should be completed as a merge"
-- MAGIC metrics = last_tx.select("operationMetrics").first()[0]
-- MAGIC assert metrics["numOutputRows"] == "3", "Make sure you only insert delicious beans"
-- MAGIC assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
-- MAGIC assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
-- MAGIC assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Dropping Tables
-- MAGIC 
-- MAGIC When working with managed Delta Lake tables, dropping a table results in permanently deleting access to the table and all underlying data files.
-- MAGIC 
-- MAGIC **NOTE**: Later in the course, we'll learn about external tables, which approach Delta Lake tables as a collection of files and have different persistence guarantees.
-- MAGIC 
-- MAGIC In the cell below, write a query to drop the **`beans`** table.

-- COMMAND ----------

DROP TABLE beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to assert that your table no longer exists.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Wrapping Up
-- MAGIC 
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
