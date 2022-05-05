# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Technical Setup notebook. Hide this cell results
# MAGIC Initialize dataset to the current user and cleanup data when reset_all_data is set to true
# MAGIC 
# MAGIC Do not edit

# COMMAND ----------

users_data = "/FileStore/Databricks_workshop/Data/Users"
spend_data = "/FileStore/Databricks_workshop/Data/User_Spend"
users_update = "/FileStore/Databricks_workshop/Data/Users_Update"

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("db_prefix", "retail", "Database prefix")
dbutils.widgets.text("min_dbr_version", "9.1", "Min required DBR version")

# COMMAND ----------

def get_cloud_name():
  return spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
from pyspark.sql.functions import to_date, col, regexp_extract, rand, to_timestamp, initcap, sha1
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name
import re
from pyspark.sql.functions import pandas_udf, PandasUDFType
import os
import pandas as pd
from time import sleep

spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "10")
#spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

db_prefix = dbutils.widgets.get("db_prefix")

dbName = current_user_no_at

cloud_storage_path = f"/Users/{current_user}/databricks_workshop/{db_prefix}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)
    
spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")

print("using cloud_storage_path {}".format(cloud_storage_path))

# COMMAND ----------

if reset_all:
   (
   spark
    .readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation",cloud_storage_path + "/users_update_schema" )
      .option("header", "true")      
      .load(users_update)
    .writeStream.format("delta")
      .option("checkpointLocation", cloud_storage_path + "/checkpoint_users_update")
    .trigger(once=True)
    .table("users_update")
 # To run autoloader in Stream mode, comment trigger option and uncomment start option below
 #  .start("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/Tables/autoloader").awaitTermination()
    )
