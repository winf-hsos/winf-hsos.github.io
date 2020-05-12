-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Extract and analyze emojis
-- MAGIC ---
-- MAGIC Emojis or emoticons are a popular way to express emotions, especially in social networks or forums. Therefore it can be interesting to analyze the use of emojis systematically, for example in the context of sentiment analysis.
-- MAGIC 
-- MAGIC **Functions we'll learn/use in this notebook:**<br><br>
-- MAGIC 
-- MAGIC - `findEmoticons()` - a user defined function to extract all emoticons into a new column of the type array
-- MAGIC - `left join` to apply a mapping table imported from Google Sheets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Naive approach using `like`
-- MAGIC ---
-- MAGIC To filter tweets for a specific emojis or a small set of emojis, the `like` operator is sufficient. An emojis is another character:

-- COMMAND ----------

select text
from tweets
where text like '%😂%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a UDF to extract emojis
-- MAGIC ---
-- MAGIC If we know a little scala (or Python), we can apply a better and more flexible way to extract emojis (and filter tweets for certain emojis). We can create our own function and register it with SQL, so that we can use the function in a normal SQL statement. In Scala, the function makes use of the regular group expression for emoticons and extracts all occurences in a tweet into a new array.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def findEmoticons(s: String): Array[String] = {
-- MAGIC   val str = Option(s).getOrElse(return Array())
-- MAGIC    """\p{block=Emoticons}""".r.findAllIn(str).toArray 
-- MAGIC }
-- MAGIC 
-- MAGIC val findEmoticonsUDF = udf[Array[String], String](findEmoticons)
-- MAGIC spark.udf.register("findEmoticons", findEmoticonsUDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once registered, we apply the function `findEmoticons` from an SQL statement:

-- COMMAND ----------

select text
      ,findEmoticons(text) as `emojis` 
from tweets
where size(findEmoticons(text)) > 0
order by size(findEmoticons(text)) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Include even more symbols
-- MAGIC ---
-- MAGIC If we need even more symbols, like foods, we can add more regular group expressions to our UDF:

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def findEmoticons(s: String): Array[String] = {
-- MAGIC   val str = Option(s).getOrElse(return Array())
-- MAGIC    """[\p{block=Emoticons}\p{block=Dingbats}\p{block=Miscellaneous Symbols And Pictographs}]""".r.findAllIn(str).toArray 
-- MAGIC }
-- MAGIC 
-- MAGIC val findEmoticonsUDF = udf[Array[String], String](findEmoticons)
-- MAGIC spark.udf.register("findEmoticons", findEmoticonsUDF)

-- COMMAND ----------

select text
      ,findEmoticons(text) as `emojis & more` 
from tweets
where size(findEmoticons(text)) > 0
order by size(findEmoticons(text)) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode the emojis array into rows
-- MAGIC ---
-- MAGIC Just as we did with words, it is helpful to transform the array of emoticons for each tweet into separate rows. In this format, we have the full power of SQL to analyze the emoticons (aggregate, filter). And we can easily apply a mapping table, for example one that contains a sentiment for each emoticon.

-- COMMAND ----------

-- Explode emoticons array into separate rows
select text
      ,explode(emojis) as emoji
from (
  select text, findEmoticons(text) as `emojis` 
  from tweets
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a view for emojis

-- COMMAND ----------

create or replace temporary view tweets_emojis as
select text
      ,explode(emojis) as emoji
from (
  select text, findEmoticons(text) as `emojis` 
  from tweets
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create and import a mapping table in Google Sheets

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "emoji_meaning"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTqChTs7Na_R4x3v-2z3BCpnazVhgyDtxHApJag0k4IGekU_74gqA8Vg-OzXRLUlYD4BPtH2rJ1Okpt/pub?output=csv"
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
-- MAGIC ## Check whether the import was successfull

-- COMMAND ----------

select * from emoji_meaning

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Apply the table to the emojis in our tweets

-- COMMAND ----------

select t.text
      ,m.meaning
      ,m.sentiment
from tweets_emojis t
left join emoji_meaning m
  on t.emoji = m.emoji
  
-- Keep only emojis that are in the meaning table
where m.emoji is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Be creative...
-- MAGIC ---
-- MAGIC There are endless ways how you can use the above technique to create meaningful analyis. I am looking forward to hearing about your ideas...
