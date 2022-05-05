# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS parquet_workshop (
# MAGIC      id                 STRING NOT NULL,
# MAGIC      email              STRING,
# MAGIC      firstname          STRING,
# MAGIC      lastname           STRING,
# MAGIC      address            STRING,
# MAGIC      city               STRING,
# MAGIC      creation_date      STRING,
# MAGIC      last_activity_date STRING,
# MAGIC      last_ip            STRING,
# MAGIC      postcode           STRING
# MAGIC   ) using parquet 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  orc_workshop(
# MAGIC      id                 STRING NOT NULL,
# MAGIC      email              STRING,
# MAGIC      firstname          STRING,
# MAGIC      lastname           STRING,
# MAGIC      address            STRING,
# MAGIC      city               STRING,
# MAGIC      creation_date      STRING,
# MAGIC      last_activity_date STRING,
# MAGIC      last_ip            STRING,
# MAGIC      postcode           STRING
# MAGIC   ) using orc 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta_workshop (
# MAGIC      id                 STRING NOT NULL,
# MAGIC      email              STRING,
# MAGIC      firstname          STRING,
# MAGIC      lastname           STRING,
# MAGIC      address            STRING,
# MAGIC      city               STRING,
# MAGIC      creation_date      STRING,
# MAGIC      last_activity_date STRING,
# MAGIC      last_ip            STRING,
# MAGIC      postcode           STRING
# MAGIC   ) using delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta_workshop

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use sumit_prakash

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spend_silver_dlt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_gold_dlt 