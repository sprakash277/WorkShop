# Databricks notebook source
# MAGIC %md
# MAGIC # Build a Lakehouse to deliver value to your buiness lines
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fnotebook_data_pipeline&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Basic Data Ingestion pipeline to present Notebook / Lakehouse capability. COPY INTO, SQL, Python, stream. BRONZE/SILVER/GOLD. <br/><i>Usage: basic data prep / exploration demo / Lakehouse presentation.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["Global Sales Report"],
# MAGIC                  "Queries": ["Customer Satisfaction Evolution"],
# MAGIC                  "DLT": ["DLT customer SQL"]},
# MAGIC          "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC                       "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,1:  Set the Env variable for the Exercise
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Preparing the data with Delta Lake
# MAGIC To deliver final outcomes such as segmentation, product classification or forecasting, we need to gather, process and clean the incoming data.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-pipeline.png" style="height: 400px"/>
# MAGIC 
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC <div style="float:left"> 
# MAGIC   
# MAGIC This can be challenging with traditional systems due to the following:
# MAGIC  * Data quality issue
# MAGIC  * Running concurrent operation
# MAGIC  * Running DELETE/UPDATE/MERGE over files
# MAGIC  * Governance (time travel, security, sharing data change, schema evolution)
# MAGIC  * Performance (lack of index, ingesting millions of small files on cloud storage)
# MAGIC  * Processing & analysing unstructured data (image, video...)
# MAGIC  * Switching between batch or streaming depending of your requirement
# MAGIC   
# MAGIC </div>
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" width="200px" style="margin: 50px 0px 0px 50px"/>
# MAGIC 
# MAGIC <br style="clear: both"/>
# MAGIC 
# MAGIC ## Solving these challenges with Delta Lake
# MAGIC 
# MAGIC 
# MAGIC **What's Delta Lake? It's a new OSS standard to bring SQL Transactional database capabilities on top of parquet files!**
# MAGIC 
# MAGIC Delta Lake simplifies data manipulation and improves team efficiency by removing Data Engineering technical pains.
# MAGIC 
# MAGIC In addition to performance boost, it also provides functionalities required in most ETL pipelines, including ACID transaction, support for DELETE / UPDATE / MERGE operation using SQL or python, Time travel, clone...
# MAGIC <!-- 
# MAGIC * **ACID transactions** (Multiple writers can simultaneously modify a data set)
# MAGIC * **Full DML support** (UPDATE/DELETE/MERGE)
# MAGIC * **BATCH and STREAMING** support
# MAGIC * **Data quality** (expectatiosn, Schema Enforcement, Inference and Evolution)
# MAGIC * **TIME TRAVEL** (Look back on how data looked like in the past)
# MAGIC * **Performance boost** with ZOrder, data skipping and Caching, solves small files issue  -->

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)2: Gold table -  users joined with their spend score
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step4.png"/>
# MAGIC 
# MAGIC We can now join the 2 tables based on the customer ID to create our final gold table.
# MAGIC 
# MAGIC This time we'll do that as an incremental batch (not the `.trigger(once=True)` setting). <br><br>
# MAGIC 
# MAGIC <div style="color:green;">
# MAGIC (1) In this excercise we will join  user_silver and spend_silver delta table to build Gold table that contains information about Users Spend. <br>
# MAGIC (2) We will Join user_silver and spend_silver table on the column "id" <br> 
# MAGIC (3) Drop the _rescued_data column that got added as part of populating spend_silver table. <br>
# MAGIC (4) We will use writestream to write data to the Delta format tables. <br>
# MAGIC (5) User trigger (once=True) to run stream one time. <br>
# MAGIC (6) To provide exactly one semantics and recover from all failed state , autoloader manintins the state of the stream in the checkpoint. <br>
# MAGIC (7) Writestream will write to the user_gold table  <br>
# MAGIC </div> 

# COMMAND ----------

# DBTITLE 1,Join user and spend to our gold table, in python
# Set the spend Data Frame with the spend_silver table
spend = spark.read.table(FILL_IN_THIS)

# Spark Structure Streaming to Join 2 tables and write the data to final Gold Tables
(spark.readStream.table("user_silver") 
     .join(spend,FILL_IN_THIS) 
     .drop("_rescued_data")
     .writeStream
 # Remove the trigger option to do incremental batch. In Production scenario, you want to do incremental batch.
        .trigger(once=True)
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_gold")
        .table(FILL_IN_THIS).awaitTermination())

spark.read.table("user_gold").display()

# COMMAND ----------

# DBTITLE 1,3:  Exercise -  Lets Visualize the user_gold table that we created Above
# MAGIC %sql
# MAGIC select * from FILL_IN_THIS limit 10
