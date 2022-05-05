# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

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
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Explore the dataset
# MAGIC 
# MAGIC We'll be using 1 flow of data for this example:
# MAGIC  * Customer stream (`customer id`, `email`, `firstname`, `lastname`...), delivered as JSON file in a blob storage

# COMMAND ----------

users_data = "/FileStore/Databricks_workshop/Data/Users"
spend_data = "/FileStore/Databricks_workshop/Data/User_Spend"

# COMMAND ----------

# DBTITLE 1,This is the data being delivered in our cloud storage. Let's explore the raw json files
files = dbutils.fs.ls(users_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As you can see, we have lot of small json files. Let's run a SQL query to explore the data
# MAGIC select * from json.`/FileStore/Databricks_workshop/Data/Users`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Bronze: loading data from blob storage
# MAGIC <div style="float:right">
# MAGIC   <img width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step1.png"/>
# MAGIC </div>
# MAGIC Databricks has advanced capacity such as Autoloader to easily ingest files, and support schema inference and evolution. 
# MAGIC 
# MAGIC For this example we'll use a sql `COPY INTO` command to incrementally load the data

# COMMAND ----------

schema = spark.read.json(files[0].path).schema

# COMMAND ----------

(
   spark
    .readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .schema(schema)
      .option("cloudFiles.maxFilesPerTrigger", "1")             
      .load(users_data)
    .writeStream.format("delta")
      .option("checkpointLocation", cloud_storage_path + "/checkpoint")
    .trigger(once=True)
    .table("user_bronze")
 # To run autoloader in Stream mode, comment trigger option and uncomment start option below
 #  .start("abfss://sales-data@sumitsalesdata.dfs.core.windows.net/demo_data/Tables/autoloader").awaitTermination()
 )

# COMMAND ----------

# DBTITLE 1,Our user_bronze Delta table is now ready for efficient query
# MAGIC %sql
# MAGIC SELECT count(*) as c, postcode FROM user_bronze GROUP BY postcode ORDER BY c desc limit 10;
