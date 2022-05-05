-- Databricks notebook source
CREATE INCREMENTAL LIVE TABLE users_bronze_dlt_2 (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/Users", "json",
map("cloudFiles.useNotifications", "true",
"cloudFiles.queueName","${event_grid_queue_name}",
"cloudFiles.connectionString","${storage_account_connection_string}",
"cloudFiles.includeExistingFiles","true")
) 
