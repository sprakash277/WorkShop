# Databricks notebook source
import dlt

@dlt.table
def users_bronze_dlt():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.useNotifications", "true")
      .option("cloudFiles.resourceGroup","sumit_rg_adb")
      .option("cloudFiles.subscriptionId","")
      .option("cloudFiles.tenantId","9f37a392-f0ae-4280-9796-f1864a10effc")
      .option("cloudFiles.clientId","")
      .option("cloudFiles.clientSecret", "")
      .load("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/text_table/demo_data/text_table/")
  )
