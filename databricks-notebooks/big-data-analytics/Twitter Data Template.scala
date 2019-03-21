// Databricks notebook source
// MAGIC %md
// MAGIC # Laden der Daten (einmalig ausführen)
// MAGIC ---
// MAGIC Der folgende Block unten erstellt 2 neue Tabellen in eurem Databricks-Account:<br><br>
// MAGIC 
// MAGIC - `twitter_followers`
// MAGIC - `twitter_timelines`
// MAGIC 
// MAGIC Ihr müsst diesen Block nur einmal ausführen, die Tabellen bleiben permanent in eurem Account gespeichert. Auch nachdem ihr euch ausloggt oder ein neues Cluster erstellt. Ihr könnt den Block mit dem kleinen Play-Symbol oben rechts ausführen. Alternativ könnt ihr Strg + Enter drücken, wenn ihr euch mit dem Cursor innerhalb des Blocks befindet.
// MAGIC 
// MAGIC Die beiden Tabellen enthalten die Follower sowie die Tweets zu den von eurer Gruppe identifizierten Twitter-Accounts. Es ist daher wichtig, dass ihr oben im Textfeld "Gruppencode" den Code eurer Gruppe eingegeben habt, bevor ihr den Codeblock zum Importieren der Daten ausführt.
// MAGIC 
// MAGIC ---
// MAGIC 
// MAGIC **HINWEIS:** Je nach Menge der Daten kann die Ausführung ein paar Minuten dauern.

// COMMAND ----------

dbutils.widgets.text("group_code", "", "Gruppencode")

// COMMAND ----------

import scala.sys.process._
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.session.timeZone", "GMT+1")

val tables = Array("twitter_followers", "twitter_timelines")

val file_ending = ".json.gz"
val group_code = dbutils.widgets.get("group_code")

for(t <- tables) {

  val tableName = t
  dbutils.fs.rm("file:/tmp/" + tableName + file_ending)

  "wget -P /tmp https://s3.amazonaws.com/nicolas.meseth/data+sets/ss2019/big_data_analytics/" + group_code + "/" + tableName + file_ending !!

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
   
  if(tableName == "twitter_followers") {  
      df = df.withColumn("created_at", unix_timestamp($"created_at", "E MMM dd HH:mm:ss Z yyyy").cast(TimestampType))
      df = df.withColumn("retrieved_time", ($"retrieved_time" / 1000).cast(TimestampType))
  }
  if(tableName == "twitter_timelines") {
    df = df.withColumn("created_at", unix_timestamp($"created_at", "E MMM dd HH:mm:ss Z yyyy").cast(TimestampType))
    df = df.withColumn("retrieved_time", ($"retrieved_time" / 1000).cast(TimestampType))    
  }
      
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
// MAGIC select count(1) as `Num Records`, 'followers' as `Table` from twitter_followers
// MAGIC union
// MAGIC select count(1) as `Num Records`, 'tweets' as `Table` from twitter_timelines

// COMMAND ----------

// MAGIC %md
// MAGIC Die Abfrage unten gibt euch die Anzahl Tweets pro User zurück.

// COMMAND ----------

// MAGIC %sql
// MAGIC select user, count(1) as `Number Tweets`
// MAGIC from twitter_timelines
// MAGIC group by user

// COMMAND ----------

// MAGIC %md
// MAGIC Die Abfrage unten liefert die Follower für jeden User.

// COMMAND ----------

// MAGIC %sql
// MAGIC select follower_of, count(1) as `Number Followers`
// MAGIC from twitter_followers
// MAGIC group by follower_of

// COMMAND ----------

// MAGIC %md
// MAGIC # Jetzt seid ihr an der Reihe ...
// MAGIC ---
// MAGIC Ihr könnt nun direkt unter diesem Block loslegen und die Tabellen und darin enthaltenen Daten abfragen. Viel Erfolg!
