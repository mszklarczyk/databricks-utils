# Databricks notebook source
# MAGIC %pip install databricks-sdk

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=dbutils.secrets.get(scope = 'mike', key = 'workspace-url'), token=dbutils.secrets.get(scope = 'mike', key = 'pat'))

# COMMAND ----------

all_jobs = {}
for job in w.jobs.list():
    all_jobs[job.job_id] = job
all_jobs[1090460915029261]

# COMMAND ----------

w.jobs.set_permissions()

# COMMAND ----------

w.jobs.set_permissions('1090460915029261', access_control_list=['','CAN_MANAGE', 'dapi',''])
