# Databricks notebook source
# DBTITLE 1,1. Will use the Widgets to pass in the value to the Notebook
dbutils.widgets.dropdown("reset_all_data", "true", ["true", "false"], "Reset all data")

# COMMAND ----------

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

# DBTITLE 1,2. Set the Env variable for the Exercise
# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

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

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3 : Explore the dataset
# MAGIC 
# MAGIC We'll be using 1 flow of data for this example:
# MAGIC  * Customer stream (`customer id`, `email`, `firstname`, `lastname`...), delivered as JSON file in a blob storage

# COMMAND ----------

# DBTITLE 1,4 : Set the data location to the variable
users_data = "/FileStore/Databricks_workshop/Data/Users"
spend_data = "/FileStore/Databricks_workshop/Data/User_Spend"

# COMMAND ----------

# DBTITLE 1,5:  This is the data being delivered in our cloud storage. Let's explore the raw json files
files = dbutils.fs.ls(users_data)
dbutils.fs.ls(users_data)


# COMMAND ----------

# DBTITLE 1,6 : Let's run a SQL query to explore the data
# MAGIC %sql
# MAGIC -- As you can see, we have lot of small json files. Let's run a SQL query to explore the data
# MAGIC select * from json.`/FileStore/Databricks_workshop/Data/Users`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 7 : Bronze: loading data from blob storage
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step1.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC Databricks has advanced capacity such as Autoloader to easily ingest files, and support schema inference and evolution. <br> <br> 
# MAGIC 
# MAGIC <div style="color:green;">
# MAGIC (1) Autoloader uses Spark Structured Stream, to use autoloader, we will set the format as "cloudfiles" and use readStream to ingest data from cloud files. <br>
# MAGIC (2) As we saw in step 5 , the format of the files are Json, we will pass in the json as the file format for the cloudfiles. <br>
# MAGIC (3) We can pass the schema to the Autoloader or let Autoloader infer schema from the event Stream, for simplicity we will read the scheam and pass the schema to the Autoloader. <br>
# MAGIC (4) For this example we'll use Autoloader to Ingest Data from users_data path. <br>
# MAGIC (5) We will use writestream to write data to the Delat format tables. To write table as delta format, pass on the delta as argument to the format. <br>
# MAGIC (6) To provide exactly one semantics and recover from all failed state , autoloader manintins the state of the stream in the checkpoint. <br>
# MAGIC (7) Autoloader can run as continuous stream or you trigger once as per the cron schedule, To trigger autoloader to run manually or at specified cron interval , set the trigger as (once=true). To set the autoloader as continous stream, don't set the trigger property. <br>
# MAGIC (8) We can use table option as part of Autoloader syntax to automarically create table if table doesn't exists. Let's pass in the table argument as let autoloader automatically create user_bronze  table . <br>
# MAGIC </div>  

# COMMAND ----------

# DBTITLE 1,8:  Let's Infer schema by reading the  users_data Json Files
schema = spark.read.json(files[0].path).schema

# COMMAND ----------

# DBTITLE 1,9: Exercise -   Run Autoloader to read user_data json files
(
   spark
    .readStream
      .format("cloudFiles")
      .option("cloudFiles.format", FILL_IN_THIS)
      .schema(FILL_IN_THIS)
      .option("cloudFiles.maxFilesPerTrigger", "1")             
      .load(FILL_IN_THIS)
    .writeStream.format(FILL_IN_THIS)
      .option("checkpointLocation", cloud_storage_path + "/checkpoint")
    .trigger(once=True)
    .table(FILL_IN_THIS)
 # To run autoloader in Stream mode, comment trigger option and uncomment start option below
 #  .start("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/Tables/autoloader").awaitTermination()
 )

# COMMAND ----------

# DBTITLE 1,10 : Exercise -  Our user_bronze Delta table is now ready for efficient query
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(*) as c, 
# MAGIC   postcode 
# MAGIC FROM 
# MAGIC   FILL_IN_THIS
# MAGIC GROUP BY 
# MAGIC   postcode 
# MAGIC ORDER BY 
# MAGIC   c desc 
# MAGIC limit 10;
