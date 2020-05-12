-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Load data from Google Spreadsheets
-- MAGIC ---
-- MAGIC Whether we want to enrich our data with external data, such as weather or market data, or we simply need a quick way to maintain and integrate a mapping table: Google Spreadsheets are easy to manage, share, and import into Databricks. This notebook serves as a template to load any table from a Google Spreadsheet.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load a table with scala
-- MAGIC ---
-- MAGIC As an example, let's assume we want to categorize our twitter users using an external mapping table. We create that table in Google Spreadsheets, add two columns `screen_name` and `category`, and then fill in the information for our users. Once we have done that, we load the table using the code block below.
-- MAGIC 
-- MAGIC **CAUTION**: You need to publish the table to the web. Click on "File" --> "Publish to the web" and then choose "Entire document" and "Comma-separated values (.csv)". Copy the link you get and paste it in line 9 below.
-- MAGIC 
-- MAGIC <img src="https://gblobscdn.gitbook.com/assets%2F-LIAGGBJMP1kBzA4YD-z%2F-M79UYSzQYyfca0vaoU_%2F-M79b8LsImbVYhFVbzJp%2Fpublish_google_spreadsheets.gif?alt=media&token=336b1bf8-3211-4a06-9c01-a66fbdaad023" width="50%">

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "user_mapping"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRkLRx_GDKr1AN0hdhPlqkfwEZXPLAyRz9j-t9oRcsYdusI0Id4zD8TX9PbsSTnTsirDQDK3sfHwpGu/pub?output=csv"
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
-- MAGIC 
-- MAGIC df.write.saveAsTable(tableName);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Apply the mapping table to the data

-- COMMAND ----------

select t.screen_name 
      ,m.category
      ,m.party
from tweets t
left join user_mapping m
  on t.screen_name = m.screen_name
where m.category is not null
