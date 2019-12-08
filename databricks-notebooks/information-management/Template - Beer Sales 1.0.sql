-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Preparation: Create data once
-- MAGIC ---
-- MAGIC You need to run the block below to create the table `beer_orders` in your Databricks account.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "beer_orders"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS5xqGRzI_inmoaHX-Dzq3aWCgDuo7DMHhfnY3FvnXsG1-V7RZuvW_l-9QJjhJ_eBb8tV2_gRiU290L/pub?gid=0&single=true&output=csv"
-- MAGIC 
-- MAGIC var localpath = "/tmp/" + tableName + ".csv"
-- MAGIC dbutils.fs.rm("file:" + localpath)
-- MAGIC "wget -O " + localpath + " " + url !!
-- MAGIC 
-- MAGIC dbutils.fs.mkdirs("dbfs:/datasets/gsheets")
-- MAGIC dbutils.fs.cp("file:" + localpath, "dbfs:/datasets/gsheets")
-- MAGIC 
-- MAGIC sqlContext.sql("drop table if exists " + tableName)
-- MAGIC var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/datasets/gsheets/" + tableName + ".csv");
-- MAGIC df.write.saveAsTable(tableName);

-- COMMAND ----------

-- Show the meta data for the table
describe beer_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SQL statements to answer the questions
-- MAGIC ---
-- MAGIC Add your SQL statements for each question below this block. Make sure you include the question as the title of the block.
