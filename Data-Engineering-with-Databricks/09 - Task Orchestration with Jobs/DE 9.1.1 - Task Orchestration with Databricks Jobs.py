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
# MAGIC # Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC New updates to the Databricks Jobs UI have added the ability to schedule multiple tasks as part of a job, allowing Databricks Jobs to fully handle orchestration for most production workloads.
# MAGIC 
# MAGIC Here, we'll start by reviewing the steps for scheduling a notebook as a triggered standalone job, and then add a dependent job using a DLT pipeline. 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Schedule a notebook as a Databricks Job
# MAGIC * Describe job scheduling options and differences between cluster types
# MAGIC * Review Job Runs to track progress and see results
# MAGIC * Schedule a DLT pipeline as a Databricks Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Jobs UI

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-9.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Create and configure a pipeline
# MAGIC The pipeline we create here is nearly identical to the one in the previous unit.
# MAGIC 
# MAGIC We will use it as part of a scheduled job in this lesson.
# MAGIC 
# MAGIC Execute the following cell to print out the values that will be used during the following configuration steps.

# COMMAND ----------

print_pipeline_config()    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Create and configure a pipeline
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar,
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipeline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the companion notebook called **DE 9.1.3 - DLT Job**. Alternatively, you can copy the **Notebook Path** and paste it into the field provided.
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above.<br/>
# MAGIC This should follow the pattern **`dbacademy_<username>_dewd_dlt_demo_91`**
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC 1. For **Pipeline Mode**, select **Triggered**
# MAGIC 1. Uncheck the **Enable autoscaling** box
# MAGIC 1. Set the number of workers to **`1`** (one)
# MAGIC 1. Click **Create**.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: we won't be executing this pipline directly as it will be executed by our job later in this lesson,<br/>
# MAGIC but if you want to test it real quick, you can click the **Start** button now.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Here, we'll start by scheduling the next notebook
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **`Create Job`** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **`reset`** for the task name
# MAGIC     1. Select the notebook **`DE 9.1.2 - Reset`** using the notebook picker.
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen rename the job (not the task) from **`reset`** (the defaulted value) to the **Job Name** provided for you in the previous cell.
# MAGIC 1. Click the blue **Run now** button in the top right to start the job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Chron Scheduling of Databricks Jobs
# MAGIC 
# MAGIC Note that on the right hand side of the Jobs UI, directly under the **Job Details** section is a section labeled **Schedule**.
# MAGIC 
# MAGIC Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 
# MAGIC Changing the **Schedule type** field from **Manual** to **Scheduled** will bring up a chron scheduling UI.
# MAGIC 
# MAGIC This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in chron syntax, which can be edited if custom configuration not available with the UI is needed.
# MAGIC 
# MAGIC At this time, we'll leave our job set with **Manual** scheduling.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Review Run
# MAGIC 
# MAGIC As currently configured, our single notebook provides identical performance to the legacy Databricks Jobs UI, which only allowed a single notebook to be scheduled.
# MAGIC 
# MAGIC To Review the Job Run
# MAGIC 1. Select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
# MAGIC 1. Find your job. If **the job is still running**, it will be under the **Active runs** section. If **the job finished running**, it will be under the **Completed runs** section
# MAGIC 1. Open the Output details by click on the timestamp field under the **Start time** column
# MAGIC 1. If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel. If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel
# MAGIC   
# MAGIC The notebook employs the magic command **`%run`** to call an additional notebook using a relative path. Note that while not covered in this course, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">new functionality added to Databricks Repos allows loading Python modules using relative paths</a>.
# MAGIC 
# MAGIC The actual outcome of the scheduled notebook is to reset the environment for our new job and pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC     1. Specify the **Task name** as **`dlt`**
# MAGIC     1. From **Type**, select **`Delta Live Tables pipeline`**
# MAGIC     1. Click the **Pipeline** field and select the DLT pipeline you configured previously<br/>
# MAGIC     Note: The pipeline will start with **Jobs-Demo-91** and will end with your email address.
# MAGIC     1. The **Depends on** field defaults to your previously defined task but may have renamed itself from the value **reset** that you specified previously to something like **Jobs-Demo-91-youremailaddress**.
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`reset`** task (possibly renamed to something like **Jobs-Demo-91-youremailaddress**) will be at the top, 
# MAGIC leading into your **`dlt`** task. 
# MAGIC 
# MAGIC This visualization represents the dependencies between these tasks.
# MAGIC 
# MAGIC Click **Run now** to execute your job.
# MAGIC 
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your job and pipeline is deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Review Multi-Task Run Results
# MAGIC 
# MAGIC Select the **Runs** tab again and then the most recent run under **Active runs** or **Completed runs** depending on if the job has completed or not.
# MAGIC 
# MAGIC The visualizations for tasks will update in real time to reflect which tasks are actively running, and will change colors if task failures occur. 
# MAGIC 
# MAGIC Clicking on a task box will render the scheduled notebook in the UI. 
# MAGIC 
# MAGIC You can think of this as just an additional layer of orchestration on top of the previous Databricks Jobs UI, if that helps; note that if you have workloads scheduling jobs with the CLI or REST API, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">the JSON structure used to configure and get results about jobs has seen similar updates to the UI</a>.
# MAGIC 
# MAGIC **NOTE**: At this time, DLT pipelines scheduled as tasks do not directly render results in the Runs GUI; instead, you will be directed back to the DLT Pipeline GUI for the scheduled Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
