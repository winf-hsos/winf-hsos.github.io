// Databricks notebook source
// MAGIC %md
// MAGIC # Laden der Daten (einmalig ausführen)
// MAGIC ---
// MAGIC Der folgende Block unten erstellt 3 neue Tabellen in eurem Databricks-Account:<br><br>
// MAGIC 
// MAGIC - `organic_food_uk`
// MAGIC - `organic_uk`
// MAGIC - `feel_about_cats_dogs`
// MAGIC 
// MAGIC Ihr müsst diesen Block nur einmal ausführen, die Tabellen bleiben permanent in eurem Account gespeichert. Auch nachdem ihr euch ausloggt oder ein neues Cluster erstellt. Ihr könnt den Block mit dem kleinen Play-Symbol oben rechts ausführen. Alternativ könnt ihr Strg + Enter drücken, wenn ihr euch mit dem Cursor innerhalb des Blocks befindet.
// MAGIC 
// MAGIC Die 3 Tabellen enthalten Informationen zu Tweets aus dem Zeitraum November/Dezember 2018, die von Mars extrahiert und angereichert wurden.
// MAGIC 
// MAGIC ---
// MAGIC 
// MAGIC **HINWEIS:** Die Ausführung des Datenimports kann ein paar Minuten dauern.

// COMMAND ----------

import scala.sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.types.{FloatType}
import org.apache.spark.sql.types.{LongType}
import org.apache.spark.sql.types.{DoubleType}

val tables = Array("organic_food_uk", "organic_uk", "feel_about_cats_dogs")

val file_ending = ".csv.gz"

for(t <- tables) {

  val tableName = t
  dbutils.fs.rm("file:/tmp/" + tableName + file_ending)

  "wget -P /tmp https://s3.amazonaws.com/nicolas.meseth/big_data_analytics/" + tableName + file_ending !!

  val localpath = "file:/tmp/" + tableName + file_ending
  dbutils.fs.rm("dbfs:/datasets/" + tableName + file_ending)
  dbutils.fs.mkdirs("dbfs:/datasets/")
  dbutils.fs.cp(localpath, "dbfs:/datasets/")
  display(dbutils.fs.ls("dbfs:/datasets/" +  tableName + file_ending))

  sqlContext.sql("drop table if exists " + tableName)
  var df = spark.read.option("header", "true") 
                        .option("quote", "\"")
                        .option("escape", "\"")
                        .option("delimiter", ",")
                        .csv("/datasets/" +  tableName + file_ending)
  
 // Conversions
  df = df.withColumn("article_id", $"article_id".cast(FloatType).cast(LongType))
  df = df.withColumn("external_id", $"external_id".cast(FloatType).cast(LongType))
  df = df.withColumn("external_author_id", $"external_author_id".cast(FloatType).cast(LongType))
  df = df.withColumn("publish_date", unix_timestamp($"publish_date", "MM/dd/yyyy HH:mm").cast("timestamp"))
  df = df.withColumn("harvested_date", unix_timestamp($"harvested_date", "MM/dd/yyyy HH:mm").cast("timestamp"))
      
  df.unpersist()
  df.cache()
  df.write.saveAsTable(tableName);  
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Überprüfen der Tabellen
// MAGIC ---
// MAGIC Die folgenden SQL-Abfragen geben euch die Rückmeldung, ob alles geklappt hat. Für jede Tabelle wird die Anzahl Datensätze in der Spalte `Num Records` ausgegeben.

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) as `Num Records`, 'organic_food_uk' as `Table` from organic_food_uk
// MAGIC union
// MAGIC select count(1) as `Num Records`, 'organic_uk' as `Table` from organic_uk
// MAGIC union
// MAGIC select count(1) as `Num Records`, 'feel_about_cats_dogs' as `Table` from feel_about_cats_dogs

// COMMAND ----------

// MAGIC %md
// MAGIC # Jetzt seid ihr an der Reihe ...
// MAGIC ---
// MAGIC Ihr könnt nun direkt unter diesem Block loslegen und die Tabellen und darin enthaltenen Daten abfragen. Viel Erfolg!
