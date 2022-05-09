# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# DBTITLE 1,We will Start with Setting up the environment for the Exercise
# MAGIC %run ../_resources/00-setup $reset_all_data=true

# COMMAND ----------

# DBTITLE 1,1.    DDL Syntax to Create  Parquet Format table
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

# DBTITLE 1,2.   DDL Syntax to create Orc Format table
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

# DBTITLE 1,3.    Exercise -  Create a Delta Format Table
# MAGIC %sql
# MAGIC FILL_IN_THIS EXISTS delta_workshop (
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
# MAGIC   ) using FILL_IN_THIS 

# COMMAND ----------

# DBTITLE 1,4.    Check all tables that we have created so far
# MAGIC %sql
# MAGIC Show tables

# COMMAND ----------

# DBTITLE 1,5.     Describe the Delta table that we created in Step 
# MAGIC %sql
# MAGIC desc extended  delta_workshop
