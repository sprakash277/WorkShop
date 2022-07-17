# Databricks notebook source
import re
import os

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

db_prefix = "retail"

dbName = current_user_no_at

cloud_storage_path = f"/Users/{current_user}/databricks_workshop/{db_prefix}"
dlt_target_path = cloud_storage_path + "/" + "dlt"

print("Database Name :-  {}".format(dbName))

# COMMAND ----------

# DBTITLE 1,0 - DLT CLUSTER POLICY JSON
{
  "cluster_type": {
    "type": "fixed",
    "value": "dlt"
  }
}

# COMMAND ----------

# DBTITLE 1,1 - DLT JOB JSON
{
    "clusters": [
        {
            "label": "default",
            "policy_id": "60628DC03F000865",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            }
        },
        {
            "label": "maintenance",
            "policy_id": "60628DC03F000865"
        }
    ],
    "development": true,
    "continuous": false,
    "edition": "advanced",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/DLT_PIPELINE_RUN/WorkShop/Lab/Instructor/02-Delta-Live-Table/02.1-SQL-Delta-Live-Table-Ingestion"
            }
        }
    ],
    "name": "DLT_PIPELINE",
    "storage": "/workshop/dlt_pipeline",
    "configuration": {
        "userID": "FILL_IN_USER_ID"
    },
    "target": "DLT_EXERCISE"
}


# COMMAND ----------

# DBTITLE 1,2 - DLT CDC JOB JSON
{
       "clusters": [
        {
            "label": "default",
            "policy_id": "60628DC03F000865",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            }
        },
        {
            "label": "maintenance",
            "policy_id": "60628DC03F000865"
        }
    ],
    "development": true,
    "continuous": false,
    "edition": "advanced",
    "photon": false,
    "libraries": [
        {
            "notebook": {
"path":"/Repos/DLT_PIPELINE_RUN/WorkShop/Lab/Instructor/02-Delta-Live-Table/CDC/2-Retail_DLT_CDC_sql"
            }
        }
    ],
    "name": "DLT_CDC",
    "configuration": {
        "userID": "FILL_IN_YOUR_ID",
        "source": "/tmp/demo/cdc_raw",
        "pipelines.applyChangesPreviewEnabled": "true"
    },
    "target": "DLT_EXERCISE"
}

