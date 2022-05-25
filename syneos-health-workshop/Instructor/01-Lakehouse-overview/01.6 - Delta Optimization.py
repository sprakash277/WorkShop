# Databricks notebook source
# DBTITLE 1,1:  Set the Env variable for the Exercise
# MAGIC %run ../../../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimize & Z-Ordering
# MAGIC To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. 
# MAGIC 
# MAGIC The `OPTIMIZE` command can be used to coalesce small files into larger ones.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize & VACUUM
# MAGIC 
# MAGIC Because of our multiple write / update / merge operation, our `user_bronze` tables has multiple files.
# MAGIC Let's use `OPTIMIZE` to compact all theses files.
# MAGIC 
# MAGIC Once `OPTIMIZE` is executed, we'll still keep the previous small files (they're still used for the time travel if you request a previous version of the table).
# MAGIC 
# MAGIC The `VACUUM` command will cleanup all the previous files. By default, `VACUUM` keeps a couple of days. We'll force it to delete all the previous small files (preventing us to travel back in time). 
# MAGIC 
# MAGIC It's best practice to periodically run OPTIMIZE and VACUUM on your delta table (ex: every week as a job)

# COMMAND ----------

# DBTITLE 1,13: Exploring `user_data_bronze` file layout on our blob storage
# MAGIC %python
# MAGIC #let's list all the files where our table has been physically stored. As you can see we have multiple small files
# MAGIC display(dbutils.fs.ls(f'{cloud_storage_path}/tables/user_bronze'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact all small files
# MAGIC OPTIMIZE user_bronze;
# MAGIC -- allow Delta to drop with 0 hours (security added to avoid conflict, you wouldn't drop all the history in prod)
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- delete previous versions & small files
# MAGIC VACUUM user_bronze RETAIN 0 hours; 

# COMMAND ----------

# DBTITLE 1,14: List the file after our compaction
# MAGIC %python
# MAGIC 2
# MAGIC #A single file remains!
# MAGIC 3
# MAGIC display(dbutils.fs.ls(f'{cloud_storage_path}/tables/user_bronze'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  user_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## When to use ZORDER or Partitions
# MAGIC 
# MAGIC ### ZORDER
# MAGIC 
# MAGIC Databricks [ZORDER](https://databricks.com/fr/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html) is similar to adding indexes on a subset of columns. it can be applied on columns having high cardinality (that is, a large number of distinct values such as a UUID, or timestamp).
# MAGIC 
# MAGIC Is a technique to colocate related information in the same set of files to improve query performance by reducing the amount of data that needs to be read.  If you expect a column to be commonly used in query predicates , then use `ZORDER BY`.
# MAGIC 
# MAGIC Keep in mind that the more ZORDER column you have, the less gain you'll get from it. We typically recommend to have a max number of 3 - 4 columns in the ZORDER clause.
# MAGIC 
# MAGIC ### Adding Partitions
# MAGIC 
# MAGIC Partitions on the other hands require to have a much lower cardinality. Partitioning on UUID will create thousands of folders creating thousands of small files, drastically impacting your read performances.
# MAGIC Your partition size should have more than 1GB of data. As example, a good partition could be the YEAR computed from a timestamp column.
# MAGIC 
# MAGIC When ensure, prefer ZORDER over partitions as you'll never fall into small files issue with ZORDER (when you don't have partition)
# MAGIC 
# MAGIC *Note: Zorder and partition can work together, ex: partition on the YEAR, ZORDER on UUID.*

# COMMAND ----------

# DBTITLE 1,Adding a ZORDER on multiple column
# MAGIC %sql
# MAGIC OPTIMIZE user_silver ZORDER BY firstname, postcode

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the metrics show that we removed more files than we added. 
# MAGIC 
# MAGIC That's all we have to do, our table will now have very fast response time if you send a request by nm_steps or calories_burnt!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_silver where postcode > 50000 
