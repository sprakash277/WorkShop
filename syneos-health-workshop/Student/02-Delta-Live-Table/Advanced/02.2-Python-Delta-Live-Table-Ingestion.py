# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducing Delta Live Table
# MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC 
# MAGIC <div><img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-pipeline.png"/></div>
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_ingestion_sql&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Delta Live Table example in SQL. BRONZE/SILVER/GOLD. Expectations to track data quality. Load model from MLFLow registry and call it to apply customer segmentation as last step.<br/><i>Usage: basic DLT demo / Lakehouse presentation.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"DLT": ["DLT customer SQL"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="200px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step1.png"/>
# MAGIC </div>
# MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC 
# MAGIC Let's use it to [create our pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/95f28631-1884-425e-af69-05c3f397dd90) and ingest the raw JSON data being delivered by an external provider. 
# MAGIC 
# MAGIC *Note: DLT is available as SQL or Python, this example will use Python*

# COMMAND ----------

# DBTITLE 1,Ingest raw User stream data in incremental mode
import dlt
@dlt.table(comment="Raw user data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution")
@dlt.expect("correct_schema","_rescued_data IS NULL")
def users_bronze_dlt():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/mnt/field-demos/retail/users_json")
  )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2/ Customer Silver layer
# MAGIC <div style="float:right">
# MAGIC   <img width="200px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step2.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC The silver layer is consuming **incremental** data from the bronze one, and cleaning up some information.
# MAGIC 
# MAGIC We're also adding an expectation on the ID. As the ID will be used in the next join operation, ID should never be null and be positive 

# COMMAND ----------

# DBTITLE 1,Clean and anonymize User data
from pyspark.sql.functions import *

@dlt.table(comment="User data cleaned and anonymized for analysis.")
@dlt.expect("valid_id","id IS NOT NULL AND id > 0")
def user_silver_dlt():
  return (
    dlt.read_stream("users_bronze_dlt").select(
      col("id").cast("int"),
      sha1("email").alias("email"),
      to_timestamp(col("creation_date"),"MM-dd-yyyy HH:mm:ss").alias("creation_date"),
      to_timestamp(col("last_activity_date"),"MM-dd-yyyy HH:mm:ss").alias("last_activity_date"),
      "firstname", 
      "lastname", 
      "address", 
      "city", 
      "last_ip", 
      "postcode"
    )
  )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ Ingest spend information with Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="200px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step3.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC Just like we did with the JSON information from the customer, autoloader can be used to load data frol tge CSV we'll received.
# MAGIC 
# MAGIC We're also adding an expectation on the ID column as we'll join the 2 tables based on this field, and we want to track it's data quality

# COMMAND ----------

# DBTITLE 1,Ingest user spending score
@dlt.table(comment="Spending score from raw data")
@dlt.expect("valid_id","id IS NOT NULL AND id > 0")
def spend_silver_dlt():
  return(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaHints","id int, age int, annual_income float, spending_core float")
    .load("/mnt/field-demos/retail/spend_csv")
  )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 4/ Joining the 2 tables to create the gold layer
# MAGIC <div style="float:right">
# MAGIC   <img width="200px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step4.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC We can now join the 2 tables on customer ID to create our final gold table.
# MAGIC 
# MAGIC As our ML model will be using `age`, `annual_income` and `spending_score` we're adding expectation to only keep valid entries 

# COMMAND ----------

# DBTITLE 1,Join both data to create our final table
@dlt.table(comment="Finale user table with all information for Analysis / ML")
@dlt.expect_all_or_drop({"valid_age":"age IS NOT NULL", "valid_income":"annual_income IS NOT NULL", "valid_score":"spending_core IS NOT NULL"})
def user_gold_dlt():
  return dlt.read_stream("user_silver_dlt").join(dlt.read("spend_silver_dlt"), ["id"], "left")

# COMMAND ----------

# Uncomment this for Customer Segmentation Model
#%pip install mlflow

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5/ Enriching the gold data with a ML model
# MAGIC <div style="float:right">
# MAGIC   <img width="200px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step5.png"/>
# MAGIC </div>
# MAGIC Our Data scientist team has build a customer segmentation model and saved it into Databricks Model registry. 
# MAGIC 
# MAGIC We can easily load this model and enrich our data with our customer segment information. Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

# COMMAND ----------

# DBTITLE 1,Load the model as a python UDF
# Uncomment this for Customer Segmentation Model
#import mlflow

#model_name = "field_demos_customer_segmentation"
#model_uri = f"models:/{model_name}/Production"
#get_customer_segmentation_cluster = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="string")

# COMMAND ----------

# DBTITLE 1,Calling our ML model
#@dlt.table(comment="Customer segmentation generated with our model from MLFlow registry")
#def user_segmentation_dlt():
#  return(
#   dlt.read_stream("user_gold_dlt")
#    .withColumn(
#      "segment", get_customer_segmentation_cluster("age", "annual_income", "spending_core").alias("segment")
#    )
#  )

# COMMAND ----------

# MAGIC %md ## Our pipeline is now ready!
# MAGIC 
# MAGIC Open the DLT pipeline and click on start to visualize your lineage and consume the data incrementally!

# COMMAND ----------

# MAGIC %md
# MAGIC # Checking your data quality metrics with Delta Live Table
# MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC 
# MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Building our first business dashboard with Databricks SQL
# MAGIC 
# MAGIC Let's switch to Databricks SQL to build a new dashboard based on all the data we ingested.
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
# MAGIC 
# MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/ab66e6c6-c2c5-4434-b784-ea5b02fe5eeb-sales-report?o=1444828305810485" target="_blank">Business Dashboard</a>

# COMMAND ----------

