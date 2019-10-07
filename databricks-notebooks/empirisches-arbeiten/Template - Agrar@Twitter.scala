// Databricks notebook source
// MAGIC %md
// MAGIC # 1. Vorbereitung
// MAGIC ---
// MAGIC ## Twitter-Daten aus 2018 laden

// COMMAND ----------

import scala.sys.process._
import org.apache.spark.sql.types.{TimestampType}

val tables = Array("twitterusers", "twitter_timelines")
val file_ending = ".json.gz"

for(t <- tables) {

  val tableName = t
  dbutils.fs.rm("file:/tmp/" + tableName + file_ending)

  "wget -P /tmp https://s3.amazonaws.com/nicolas.meseth/filterblase/" + tableName + file_ending !!

  val localpath = "file:/tmp/" + tableName + file_ending
  dbutils.fs.rm("dbfs:/datasets/" + tableName + file_ending)
  dbutils.fs.mkdirs("dbfs:/datasets/")
  dbutils.fs.cp(localpath, "dbfs:/datasets/")
  display(dbutils.fs.ls("dbfs:/datasets/" +  tableName + file_ending))

  sqlContext.sql("drop table if exists " + tableName)
  var df = spark.read.option("header", "true") 
                        .option("inferSchema", "true")
                        .option("quote", "\"")
                        .option("escape", "\"")
                        .json("/datasets/" +  tableName + file_ending)
  
  // Convert int to timestamp
  if(tableName == "twitter_timelines") {  
    df = df.withColumn("created_at", $"created_at".cast(TimestampType))
  }
  
    
  df.unpersist()
  df.cache()
  df.write.saveAsTable(tableName);  
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zeilen in den beiden Tabellen überprüfen

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) as `Anzahl`, 'Tweets' as `Was?` from twitter_timelines
// MAGIC union all
// MAGIC select count(distinct follower_of) as `Anzahl`, 'Users' as `Was?` from twitterusers

// COMMAND ----------

// MAGIC %md
// MAGIC # 2. Tweets mit SQL analysieren

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aufgabe 1: Wann tweeted die Branche? Erstellt eine Abfrage, die die Verteilung der Tweets nach Uhrzeit des Tages zurückliefert. Erstell eine geeignete Visualisierung!
// MAGIC ---
// MAGIC Die Funktion `hour()` kann auf eine Spalte vom Typ `timestamp` angewendet werden und gibt euch die Stunde im 24-Stunden-Format zurück.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Deine Antwort als SQL folgt hier

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aufgabe 2: Welche User haben in Summe die meisten Retweets, wenn wir nur die originären Tweets der User betrachten (keine Retweets anderer User)?

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Deine Antwort als SQL folgt hier

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aufgabe 3: Wann wurde 2018 am meisten über das Thema Dürre getweeted? Erstellt einen Balkendiagramm, das die Anzahl an Tweets zu diesem Themenkomplex pro Monat im Jahr 2018 darstellt!
// MAGIC ---
// MAGIC Der Ausdruck `date_format(created_at,'MMM YYYY')` stellt ein Datum im Format "Oct 2018", "Nov 2018" usw. dar.
// MAGIC 
// MAGIC Mit `array_contains(hashtags, 'duerre')` kann geprüft werden, ob im Feld `hashtags` der Wert "duerre" enthalten ist.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Deine Antwort als SQL folgt hier

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aufgabe 4: Welche Themen außer der Dürre waren 2018 noch relevant?

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Deine Antwort als SQL folgt hier
