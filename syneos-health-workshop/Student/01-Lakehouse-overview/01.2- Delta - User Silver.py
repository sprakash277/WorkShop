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

# DBTITLE 1,2:  Exercise -  Lets View the user_bronze table that we created in the Previous exercise
# MAGIC %sql
# MAGIC select * from FILL_IN_THIS limit 5

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3: Silver data: anonymized table, date cleaned
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step2.png"/>
# MAGIC 
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC 
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.<br><br>
# MAGIC 
# MAGIC <div style="color:green;">
# MAGIC (1) In this excercise we will create user_silver table which will have filtered data from the bronze table using Structured Streaming. <br>
# MAGIC (2) We can use Delta Table as source for Structured Streaming. <br> 
# MAGIC (3) We will be reading below columns from the user_bronze table. <br>
# MAGIC     &nbsp; &nbsp; &nbsp;(a) firstname <br>
# MAGIC     &nbsp; &nbsp; &nbsp;(b) email -> use sha1 function  to annonymize email column<br>
# MAGIC     &nbsp; &nbsp; &nbsp;(c) lastname -> initcap , Converts the first letter of each word in a string to uppercase, and converts any remaining characters in each word to lowercase <br>
# MAGIC     &nbsp; &nbsp; &nbsp;(d) creation_date <br>
# MAGIC     &nbsp; &nbsp; &nbsp;(e) last_activity_date : Transform last_activity_date to this format -> MM-dd-yyyy HH:mm:ss <br>
# MAGIC (4) We will use writestream to write data to the Delta format tables. To write table as delta format, pass on the delta as argument to the format. <br>
# MAGIC (5) User trigger (once=True) to run stream one time. <br>
# MAGIC (6) To provide exactly one semantics and recover from all failed state , autoloader manintins the state of the stream in the checkpoint. <br>
# MAGIC (7) Writestream will write to the user_silver table  <br>
# MAGIC </div> 

# COMMAND ----------

(spark.readStream 
        .FILL_IN_THIS("user_bronze")         
          .withColumn("firstname", initcap(col("firstname")))
          .withColumn("email", FILL_IN_THIS(col("email")))
          .withColumn("lastname", FILL_IN_THIS )
          .withColumn("creation_date", to_timestamp(col("creation_date"), "MM-dd-yyyy HH:mm:ss"))
          .withColumn("last_activity_date", FILL_IN_THIS )
     .writeStream
     .trigger(FILL_IN_THIS)
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_user_silver")
        .table("user_silver")
)

# COMMAND ----------

# DBTITLE 1,4:  Exercise -  Lets Visualize the user_silver table that we created Above
# MAGIC %sql
# MAGIC select * FILL_IN_THIS limit 10;
