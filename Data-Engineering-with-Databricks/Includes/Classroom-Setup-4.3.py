# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.3"

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)
load_eltwss_external_tables()
DA.conclude_setup()


# COMMAND ----------


