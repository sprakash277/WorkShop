# Databricks notebook source
# MAGIC %md ## Simplify your operations with transactional DELETE/UPDATE/MERGE operations
# MAGIC Traditional Data Lake struggle to run these simple DML operations. Using Databricks and Delta Lake, your data is stored on your blob storage with transactional capabilities. You can issue DML operation on Petabyte of data without having to worry about concurrent operations.
# MAGIC 
# MAGIC Databrick Managed Delta also provides more advanced capabilities:
# MAGIC 
# MAGIC * Time travel, table restore, 
# MAGIC * clone zero copy
# MAGIC * Performance: Index (Zorder), Auto compaction, optimize write, Low shuffle Merge...
# MAGIC * CDC (to propagate data changes)
# MAGIC * ...

# COMMAND ----------

# DBTITLE 1,1:  Set the Env variable for the Exercise
# MAGIC %run ../../../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Let's count the number of rows in user_gold table that we created previously
# MAGIC %sql
# MAGIC select count(*) from user_gold

# COMMAND ----------

# DBTITLE 1,2: Exercise -   We just realized we have to delete data before 2015-01-01, let's fix that
# MAGIC %sql 
# MAGIC DELETE FROM user_gold where creation_date < FILL_IN_THIS ;

# COMMAND ----------

# DBTITLE 1,3: Let's Visualize the user_gold table after we performed delete operations.
# MAGIC %sql
# MAGIC select count(*) from user_gold

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Viewing the table as of Version 0 shows the initial row count 
# MAGIC This is done by specifying the option `versionAsOf` as 0. When we time travel to Version 0, we see only the first month of data. 
# MAGIC 
# MAGIC We can use the `DESCRIBE HISTORY` command to see all the versions of the Delta table so far. Note that we did not have to perform any manual actions to capture this rich table history. 

# COMMAND ----------

# DBTITLE 1,4: Exercise - View the History of the user_gold Table
# MAGIC %sql
# MAGIC DESCRIBE FILL_IN_THIS user_gold

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 5. Time Travel back to version 1

# COMMAND ----------

# DBTITLE 1,5: Exercise - Restore the user_gold table to Version 1
# MAGIC %sql
# MAGIC -- We're at version 2 after our most recent append, so let's roll back to version 1
# MAGIC FILL_IN_THIS TABLE user_gold TO VERSION AS OF 1;

# COMMAND ----------

# DBTITLE 1,6: View the History of the user_gold Table
# MAGIC %sql
# MAGIC DESCRIBE HISTORY user_gold;

# COMMAND ----------

# DBTITLE 1,7: Show table data as of Version 1
# MAGIC %sql
# MAGIC select * from user_gold VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 8. Merge back into the original table
# MAGIC We do this by using a `MERGE INTO` statement. 
# MAGIC 
# MAGIC Note that with Delta, you are able to specify actions for when the match condition is met AND when the match condition is not met. 
# MAGIC This way, you get to define exactly what happens to all the data in your table. 

# COMMAND ----------

# MAGIC 
# MAGIC %md-sandbox
# MAGIC #### Merging Records without Delta vs with Delta
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2019/03/UpsertsBlog.jpg" alt="MERGE INTO" style="width: 1000px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,9: Lets Visualize the users_update that we will use for Merge to user_gold table
# MAGIC %sql
# MAGIC select * from users_update limit 10

# COMMAND ----------

# DBTITLE 1,10: Lets Visualize the users_gold before the merge happens
# MAGIC %sql
# MAGIC select * from user_gold  where id <= 12325 

# COMMAND ----------

# DBTITLE 1,Exercise - Merge user_update to user_gold on id
# MAGIC %sql
# MAGIC -- Now use MERGE INTO to update the historical table.
# MAGIC -- The users_update table has 1 new record with id 2138 and 4 updates to the address column with id 5862,8892,11807,12325
# MAGIC 
# MAGIC MERGE INTO 
# MAGIC FILL_IN_THIS AS orig 
# MAGIC USING users_update AS new 
# MAGIC ON orig.id = new.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   orig.address = FILL_IN_THIS
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# DBTITLE 1,11: Lets Visualize the users_gold Post Merge
# MAGIC %sql
# MAGIC select * from user_gold  where id <= 12325 

# COMMAND ----------

# DBTITLE 1,12: Exercise - View the History of the user_gold Table
# MAGIC %sql
# MAGIC DESCRIBE HISTORY user_gold;
