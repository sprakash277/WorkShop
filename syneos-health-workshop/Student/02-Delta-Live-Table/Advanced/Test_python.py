# Databricks notebook source
import dlt

@dlt.table
def users_bronze_dlt():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.useNotifications", "true")
      .option("cloudFiles.resourceGroup","sumit_rg_adb")
      .option("cloudFiles.subscriptionId","3f2e4d32-8e8d-46d6-82bc-5bb8d962328b")
      .option("cloudFiles.tenantId","9f37a392-f0ae-4280-9796-f1864a10effc")
      .option("cloudFiles.clientId","df710e85-3e7f-4ee6-b81e-b335b0c9d96a")
      .option("cloudFiles.clientSecret", "aJl7Q~fWUwQClq.GX~jQUn0TkEgQAnz6Gc~sW")
      .load("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/text_table/demo_data/text_table/")
  )