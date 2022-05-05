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

print("Database Name :- {}".format(dbName))
print("DLT Target Path :- {}".format(dlt_target_path))