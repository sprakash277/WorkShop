-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Introducing Delta Live Tables
-- MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC 
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC 
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC 
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC 
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC 
-- MAGIC ### Building a Delta Live Table pipeline to analyze and reduce churn
-- MAGIC 
-- MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our customers information.
-- MAGIC 
-- MAGIC We'll incrementally load new data with the autoloader, join this information and then load a model from MLFlow to perform our customer segmentation.
-- MAGIC 
-- MAGIC This information will then be used to build our DBSQL dashboard to track customer behavior and churn.
-- MAGIC 
-- MAGIC <div><img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-pipeline.png"/></div>
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_ingestion_sql&dt=DATA_PIPELINE">
-- MAGIC <!-- [metadata={"description":"Delta Live Table example in SQL. BRONZE/SILVER/GOLD. Expectations to track data quality. Load model from MLFLow registry and call it to apply customer segmentation as last step.<br/><i>Usage: basic DLT demo / Lakehouse presentation.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{"DLT": ["DLT customer SQL"]},
-- MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
-- MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC 
-- MAGIC Let's use it to [create our pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/95f28631-1884-425e-af69-05c3f397dd90) and ingest the raw JSON data being delivered by an external provider. 

-- COMMAND ----------

-- DBTITLE 1,Ingest raw User stream data in incremental mode
CREATE INCREMENTAL LIVE TABLE ${userID}_users_bronze_dlt (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
-- AS SELECT * FROM cloud_files("${users_data}", "json")
AS SELECT * FROM cloud_files("/FileStore/Databricks_workshop/Data/Users", "json")

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Customer Silver layer
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step2.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC The silver layer is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC 
-- MAGIC We're also adding an expectation on the ID. As the ID will be used in the next join operation, ID should never be null and be positive 

-- COMMAND ----------

-- DBTITLE 1,Clean and anonymise User data
CREATE INCREMENTAL LIVE TABLE ${userID}_user_silver_dlt (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0) 
)
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT
  cast(id as int), 
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  firstname, 
  lastname, 
  address, 
  city, 
  last_ip, 
  postcode
from STREAM(live.${userID}_users_bronze_dlt)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Ingest spend information with Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step3.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC Just like we did with the JSON information from the customer, autoloader can be used to load data from tge CSV we'll receive.
-- MAGIC 
-- MAGIC We're also adding an expectation on the ID column as we'll join the 2 tables based on this field, and we want to track it's data quality

-- COMMAND ----------

-- DBTITLE 1,Ingest user spending score
CREATE INCREMENTAL LIVE TABLE ${userID}_spend_silver_dlt (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0)
)
COMMENT "Spending score from raw data"
AS SELECT * FROM cloud_files("/FileStore/Databricks_workshop/Data/User_Spend", "csv", map("cloudFiles.schemaHints", "id int, age int, annual_income float, spending_core float"))
--AS SELECT * FROM cloud_files("${spend_data}", "csv", map("cloudFiles.schemaHints", "id int, age int, annual_income float, spending_core float"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 4/ Joining the 2 tables to create the gold layer
-- MAGIC 
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step4.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC We can now join the 2 tables on customer ID to create our final gold table.
-- MAGIC 
-- MAGIC As our ML model will be using `age`, `annual_income` and `spending_score` we're adding expectation to only keep valid entries 

-- COMMAND ----------

-- DBTITLE 1,Join both data to create our final table
CREATE INCREMENTAL LIVE TABLE ${userID}_user_gold_dlt (
  CONSTRAINT valid_age EXPECT (age IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_income EXPECT (annual_income IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_score EXPECT (spending_core IS NOT NULL ) ON VIOLATION DROP ROW
)
COMMENT "Finale user table with all information for Analysis / ML"
AS SELECT * FROM STREAM(live.${userID}_user_silver_dlt) LEFT JOIN live.${userID}_spend_silver_dlt USING (id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Checking your data quality metrics with Delta Live Tables
-- MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Building our first business dashboard with Databricks SQL
-- MAGIC 
-- MAGIC Let's switch to Databricks SQL to build a new dashboard based on all the data we ingested.
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
