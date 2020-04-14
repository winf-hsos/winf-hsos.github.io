-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create the API Code Input
-- MAGIC ---
-- MAGIC The following lines makes sure that the input field for your group's API code is present.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("api_key", "", "API Code")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Import the Twitter Data
-- MAGIC ---
-- MAGIC You need run the following code block to import a new version of your twitter data. After you ran the code, you should have a table `tweets` that you can query with SQL.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC import org.apache.spark.sql.types.{TimestampType}
-- MAGIC import org.apache.spark.sql.types.{IntegerType}
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC spark.conf.set("spark.sql.session.timeZone", "GMT+2")
-- MAGIC 
-- MAGIC val tables = Array("tweets")
-- MAGIC 
-- MAGIC val file_ending = ".json.gz"
-- MAGIC //val group_code = dbutils.widgets.get("group_code")
-- MAGIC val api_key = dbutils.widgets.get("api_key")
-- MAGIC 
-- MAGIC for(t <- tables) {
-- MAGIC 
-- MAGIC   val tableName = t
-- MAGIC   var fileName = "tweets_" + api_key + file_ending
-- MAGIC   val localpath = "file:/tmp/" + fileName
-- MAGIC   dbutils.fs.rm("file:/tmp/" + fileName)
-- MAGIC 
-- MAGIC   var url = "https://firebasestorage.googleapis.com/v0/b/big-data-analytics-helper.appspot.com/o/" + fileName +"?alt=media"  
-- MAGIC 
-- MAGIC   "wget " + url + " -O /tmp/" +  fileName !!
-- MAGIC   
-- MAGIC   dbutils.fs.rm("dbfs:/datasets/" + fileName)
-- MAGIC   dbutils.fs.mkdirs("dbfs:/datasets/")
-- MAGIC   dbutils.fs.cp(localpath, "dbfs:/datasets/")
-- MAGIC     
-- MAGIC   display(dbutils.fs.ls("dbfs:/datasets/" +  fileName))  
-- MAGIC 
-- MAGIC   sqlContext.sql("drop table if exists " + tableName)
-- MAGIC   var df = spark.read.option("inferSchema", "true")
-- MAGIC                      .option("quote", "\"")
-- MAGIC                      .option("escape", "\\")
-- MAGIC                      .json("/datasets/" + fileName)
-- MAGIC  
-- MAGIC   if(tableName == "tweets") {
-- MAGIC     df = df.withColumn("created_at", unix_timestamp($"created_at", "E MMM dd HH:mm:ss Z yyyy").cast(TimestampType))
-- MAGIC     df = df.withColumn("insert_timestamp", unix_timestamp($"insert_timestamp", "yyyy-MM-dd HH:mm:ss.SSS Z").cast(TimestampType))
-- MAGIC     df = df.withColumn("favorite_count", $"favorite_count".cast(IntegerType))
-- MAGIC     df = df.withColumn("retweet_count", $"retweet_count".cast(IntegerType))
-- MAGIC   }
-- MAGIC       
-- MAGIC   df.unpersist()
-- MAGIC   df.cache()
-- MAGIC   df.write.saveAsTable(tableName);
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Check tweets table
-- MAGIC ---
-- MAGIC A quick check whether the import was successful.

-- COMMAND ----------

select count(*) from tweets
