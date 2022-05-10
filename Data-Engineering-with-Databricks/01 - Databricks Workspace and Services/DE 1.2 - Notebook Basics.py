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
# MAGIC # Notebook Basics
# MAGIC 
# MAGIC Notebooks are the primary means of developing and executing code interactively on Databricks. This lesson provides a basic introduction to working with Databricks notebooks.
# MAGIC 
# MAGIC If you've previously used Databricks notebooks but this is your first time executing a notebook in Databricks Repos, you'll notice that basic functionality is the same. In the next lesson, we'll review some of the functionality that Databricks Repos adds to notebooks.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Attach a notebook to a cluster
# MAGIC * Execute a cell in a notebook
# MAGIC * Set the language for a notebook
# MAGIC * Describe and use magic commands
# MAGIC * Create and run a SQL cell
# MAGIC * Create and run a Python cell
# MAGIC * Create a markdown cell
# MAGIC * Export a Databricks notebook
# MAGIC * Export a collection of Databricks notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Attach to a Cluster
# MAGIC 
# MAGIC In the previous lesson, you should have either deployed a cluster or identified a cluster that an admin has configured for you to use.
# MAGIC 
# MAGIC Directly below the name of this notebook at the top of your screen, use the drop-down list to connect this notebook to your cluster.
# MAGIC 
# MAGIC **NOTE**: Deploying a cluster can take several minutes. A green arrow will appear to the right of the cluster name once resources have been deployed. If your cluster has a solid gray circle to the left, you will need to follow instructions to <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">start a cluster</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Notebooks Basics
# MAGIC 
# MAGIC Notebooks provide cell-by-cell execution of code. Multiple languages can be mixed in a notebook. Users can add plots, images, and markdown text to enhance their code.
# MAGIC 
# MAGIC Throughout this course, our notebooks are designed as learning instruments. Notebooks can be easily deployed as production code with Databricks, as well as providing a robust toolset for data exploration, reporting, and dashboarding.
# MAGIC 
# MAGIC ### Running a Cell
# MAGIC * Run the cell below using one of the following options:
# MAGIC   * **CTRL+ENTER** or **CTRL+RETURN**
# MAGIC   * **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
# MAGIC   * Using **Run Cell**, **Run All Above** or **Run All Below** as seen here<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>

# COMMAND ----------

print("I'm running Python! :)")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC **NOTE**: Cell-by-cell code execution means that cells can be executed multiple times or out of order. Unless explicitly instructed, you should always assume that the notebooks in this course are intended to be run one cell at a time from top to bottom. If you encounter an error, make sure you read the text before and after a cell to ensure that the error wasn't an intentional learning moment before you try to troubleshoot. Most errors can be resolved by either running earlier cells in a notebook that were missed or re-executing the entire notebook from the top.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Setting the Default Notebook Language
# MAGIC 
# MAGIC The cell above executes a Python command, because our current default language for the notebook is set to Python.
# MAGIC 
# MAGIC Databricks notebooks support Python, SQL, Scala, and R. A language can be selected when a notebook is created, but this can be changed at any time.
# MAGIC 
# MAGIC The default language appears directly to the right of the notebook title at the top of the page. Throughout this course, we'll use a blend of SQL and Python notebooks.
# MAGIC 
# MAGIC We'll change the default language for this notebook to SQL.
# MAGIC 
# MAGIC Steps:
# MAGIC * Click on the **Python** next to the notebook title at the top of the screen
# MAGIC * In the UI that pops up, select **SQL** from the drop down list 
# MAGIC 
# MAGIC **NOTE**: In the cell just before this one, you should see a new line appear with <strong><code>&#37;python</code></strong>. We'll discuss this in a moment.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Create and Run a SQL Cell
# MAGIC 
# MAGIC * Highlight this cell and press the **B** button on the keyboard to create a new cell below
# MAGIC * Copy the following code into the cell below and then run the cell
# MAGIC 
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC 
# MAGIC **NOTE**: There are a number of different methods for adding, moving, and deleting cells including GUI options and keyboard shortcuts. Refer to the <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">docs</a> for details.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Magic Commands
# MAGIC * Magic commands are specific to the Databricks notebooks
# MAGIC * They are very similar to magic commands found in comparable notebook products
# MAGIC * These are built-in commands that provide the same outcome regardless of the notebook's language
# MAGIC * A single percent (%) symbol at the start of a cell identifies a magic command
# MAGIC   * You can only have one magic command per cell
# MAGIC   * A magic command must be the first thing in a cell

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Language Magics
# MAGIC Language magic commands allow for the execution of code in languages other than the notebook's default. In this course, we'll see the following language magics:
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC 
# MAGIC Adding the language magic for the currently set notebook type is not necessary.
# MAGIC 
# MAGIC When we changed the notebook language from Python to SQL above, existing cells written in Python had the <strong><code>&#37;python</code></strong> command added.
# MAGIC 
# MAGIC **NOTE**: Rather than changing the default language of a notebook constantly, you should stick with a primary language as the default and only use language magics as necessary to execute code in another language.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Markdown
# MAGIC 
# MAGIC The magic command **&percnt;md** allows us to render Markdown in a cell:
# MAGIC * Double click this cell to begin editing it
# MAGIC * Then hit **`Esc`** to stop editing
# MAGIC 
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC 
# MAGIC This is a test of the emergency broadcast system. This is only a test.
# MAGIC 
# MAGIC This is text with a **bold** word in it.
# MAGIC 
# MAGIC This is text with an *italicized* word in it.
# MAGIC 
# MAGIC This is an ordered list
# MAGIC 0. once
# MAGIC 0. two
# MAGIC 0. three
# MAGIC 
# MAGIC This is an unordered list
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC 
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC 
# MAGIC Images:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC 
# MAGIC And of course, tables:
# MAGIC 
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### %run
# MAGIC * You can run a notebook from another notebook by using the magic command **%run**
# MAGIC * Notebooks to be run are specified with relative paths
# MAGIC * The referenced notebook executes as if it were part of the current notebook, so temporary views and other local declarations will be available from the calling notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Uncommenting and executing the following cell will generate the following error:<br/>
# MAGIC **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC But we can declare it and a handful of other variables and functions buy running this cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The **`../Includes/Classroom-Setup-1.2`** notebook we referenced includes logic to create and **`USE`** a database, as well as creating the temp view **`demo_temp_vw`**.
# MAGIC 
# MAGIC We can see this temp view is now available in our current notebook session with the following query.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC We'll use this pattern of "setup" notebooks throughout the course to help configure the environment for lessons and labs.
# MAGIC 
# MAGIC These "provided" variables, functions and other objects should be easily identifiable in that they are part of the **`DA`** object which is an instance of **`DBAcademyHelper`**.
# MAGIC 
# MAGIC With that in mind, most lessons will use variables derived from your username to organize files and databases. 
# MAGIC 
# MAGIC This pattern allows us to avoid collision with other users in shared a workspace.
# MAGIC 
# MAGIC The cell below uses Python to print some of those variables previously defined in this notebook's setup script:

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.db_name:           {DA.db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC In addition to this, these same variables are "injected" into the SQL context so that we can use them in SQL statements.
# MAGIC 
# MAGIC We will talk more about this later, but you can see a quick example in the following cell.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Note the subtle but important difference in the casing of the word **`da`** and **`DA`** in these two examples.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.db_name}' as database_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Databricks Utilities
# MAGIC Databricks notebooks provide a number of utility commands for configuring and interacting with the environment: <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC 
# MAGIC Throughout this course, we'll occasionally use **`dbutils.fs.ls()`** to list out directories of files from Python cells.

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## display()
# MAGIC 
# MAGIC When running SQL queries from cells, results will always be displayed in a rendered tabular format.
# MAGIC 
# MAGIC When we have tabular data returned by a Python cell, we can call **`display`** to get the same type of preview.
# MAGIC 
# MAGIC Here, we'll wrap the previous list command on our file system with **`display`**.

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The **`display()`** command has the following capabilities and limitations:
# MAGIC * Preview of results limited to 1000 records
# MAGIC * Provides button to download results data as CSV
# MAGIC * Allows rendering plots

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Downloading Notebooks
# MAGIC 
# MAGIC There are a number of options for downloading either individual notebooks or collections of notebooks.
# MAGIC 
# MAGIC Here, you'll go through the process to download this notebook as well as a collection of all the notebooks in this course.
# MAGIC 
# MAGIC ### Download a Notebook
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the **File** option to the right of the cluster selection at the top of the notebook
# MAGIC * From the menu that appears, hover over **Export** and then select **Source File**
# MAGIC 
# MAGIC The notebook will download to your personal laptop. It will be named with the current notebook name and have the file extension for the default language. You can open this notebook with any file editor and see the raw contents of Databricks notebooks.
# MAGIC 
# MAGIC These source files can be uploaded into any Databricks workspace.
# MAGIC 
# MAGIC ### Download a Collection of Notebooks
# MAGIC 
# MAGIC **NOTE**: The following instructions assume you have imported these materials using **Repos**.
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the  ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** on the left sidebar
# MAGIC   * This should give you a preview of the parent directories for this notebook
# MAGIC * On the left side of the directory preview around the middle of the screen, there should be a left arrow. Click this to move up in your file hierarchy.
# MAGIC * You should see a directory called **Data Engineering with Databricks**. Click the the down arrow/chevron to bring up a menu
# MAGIC * From the menu, hover over **Export** and select **DBC Archive**
# MAGIC 
# MAGIC The DBC(Databricks Cloud) file that is downloaded contains a zipped collection of the directories and notebooks in this course. Users should not attempt to edit these DBC files locally, but they can be safely uploaded into any Databricks workspace to move or share notebook contents.
# MAGIC 
# MAGIC **NOTE**: When downloading a collection of DBCs, result previews and plots will also be exported. When downloading source notebooks, only code will be saved.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Learning More
# MAGIC 
# MAGIC We like to encourage you to explore the documentation to learn more about the various features of the Databricks platform and notebooks.
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">User Guide / Notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importing notebooks - Supported Formats</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Administration Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Cluster Configuration</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Release Notes</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## One more note! 
# MAGIC 
# MAGIC At the end of each lesson you will see the following command, **`DA.cleanup()`**.
# MAGIC 
# MAGIC This method drops lesson-specific databases and working directories in an attempt to keep your workspace clean and maintain the immutability of each lesson.
# MAGIC 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
