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
      .option("cloudFiles.tenantId","")
      .option("cloudFiles.clientId","")
      .option("cloudFiles.clientSecret", "")
      .load("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/text_table/demo_data/text_table/")
  )
