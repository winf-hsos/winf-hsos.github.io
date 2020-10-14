-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Load Covid-19 Data from Our World in Data
-- MAGIC ---
-- MAGIC <a href="https://ourworldindata.org/" target="_blank">Our World in Data</a> is an extensive resource for data sets around various topics. During the corona pandemic, they provide an frequently updated data set about the pandemic. This notebook is a template to load this data into your Databricks account and query it with SQL.
-- MAGIC 
-- MAGIC Find documentation about the data set here:<br><br>
-- MAGIC 
-- MAGIC - <a href="https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data-codebook.md" target="_blank">OWID Covid Data Codebook</a>
-- MAGIC - <a href="https://github.com/owid/covid-19-data/tree/master/public/data" target="_blank">OWID Covid Data hosted on Github</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load the data directly from OWID website
-- MAGIC ---
-- MAGIC The following code block retrieves the latest version of the data set and loads into a table called `owid_covid`:

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "owid_covid"
-- MAGIC 
-- MAGIC // URL to covid-19 datatset as CSV
-- MAGIC var url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
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

-- MAGIC %md
-- MAGIC ## What does the data set contain?

-- COMMAND ----------

describe owid_covid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Sample Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How many new deaths were registered in Germany per day?

-- COMMAND ----------

select `location`
       ,new_deaths 
       ,date_format(date, 'yyyy-MM-dd') as `Day`
from owid_covid
where location = "Germany"
and date >= "2020-03-01"
order by `Day`
