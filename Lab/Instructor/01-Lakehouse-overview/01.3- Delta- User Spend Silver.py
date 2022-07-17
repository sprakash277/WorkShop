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
# MAGIC %run ../../../_resources/00-setup $reset_all_data=false

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

# DBTITLE 1,2 : Set the data location to the variable
users_data = "/FileStore/Databricks_workshop/Data/Users"
spend_data = "/FileStore/Databricks_workshop/Data/User_Spend"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Ingest Customer Spend data
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step3.png"/>
# MAGIC 
# MAGIC Our customer spend information is delivered by the finance team on another stream in csv file. Let's ingest this data using the auto-loader as previously: <br> <br> 
# MAGIC 
# MAGIC <div style="color:green;">
# MAGIC (1) Autoloader uses Spark Structured Stream, to use autoloader, we will set the format as "cloudfiles" and use readStream to ingest data from cloud files. <br>
# MAGIC (2) The format of the files are csv, we will pass in the csv as the file format for the cloudfiles. <br>
# MAGIC (3) For schema we can pass on the schema as SchemaHints, and would let Autoloader infer Schema <br>
# MAGIC (4) For this example we'll use Autoloader to Ingest Data from spend_data path. <br>
# MAGIC (5) We will use writestream to write data to the Delat format tables. To write table as delta format, pass on the delta as argument to the format. <br>
# MAGIC (6) To provide exactly one semantics and recover from all failed state , autoloader manintins the state of the stream in the checkpoint. <br>
# MAGIC (7) Autoloader can run as continuous stream or you trigger once as per the cron schedule, To trigger autoloader to run manually or at specified cron interval , set the trigger as (once=true). To set the autoloader as continous stream, don't set the trigger property. <br>
# MAGIC (8) We can use table option as part of Autoloader syntax to automarically create table if table doesn't exists. Let's pass in the table argument as let autoloader automatically create spend_silver  table . <br>
# MAGIC </div>  

# COMMAND ----------

# DBTITLE 1,Let's run the Autoloader - this time to read user spend csv file 
(spark.readStream
        .format("cloudFiles") 
        .option("cloudFiles.format", "csv") 
        .option("cloudFiles.schemaHints", "age int, annual_income int, spending_core int") #schema subset for evolution / new field
        .option("cloudFiles.maxFilesPerTrigger", "10") 
        #Autoloader will automatically infer all the schema & evolution
        .option("cloudFiles.schemaLocation", cloud_storage_path+"/schema_spend") #Autoloader will automatically infer all the schema & evolution
        .load(spend_data)
      .withColumn("id", col("id").cast("int"))
      .writeStream
        .trigger(once=True)
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_spend")
        .table("spend_silver"))

# COMMAND ----------

# DBTITLE 1,4:  Exercise -  Lets Visualize the user_silver table that we created Above
# MAGIC %sql
# MAGIC -- _rescued_data Column got added as part of previous Autoloader write. The rescued data column contains any data that wasn’t parsed, because it was missing from the given schema, because there was a type mismatch, or because the casing of the column didn’t match. The rescued data column is part of the schema returned by Auto Loader as “_rescued_data” by default when the schema is being inferred.
# MAGIC select * from spend_silver limit 10;
