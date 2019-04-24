// Databricks notebook source
// MAGIC %md
// MAGIC # Themen in Texten identifizieren
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ## Laden der Daten (einmalig ausführen)
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

val file_ending = ".json"
val group_code = dbutils.widgets.get("group_code")


for(t <- tables) {

  val tableName = t
  var localFileName = "parsed%2F" + group_code + "%2F" + tableName + file_ending + "?alt=media"
  val localpath = "file:/tmp/" + localFileName
  dbutils.fs.rm("file:/tmp/" + localFileName)

  var url = "https://firebasestorage.googleapis.com/v0/b/big-data-analytics-helper.appspot.com/o/parsed%2F" + group_code + "%2F" + tableName + file_ending + "?alt=media"  

  "wget -P /tmp " + url !!
  
  dbutils.fs.rm("dbfs:/datasets/" + tableName + file_ending)
  dbutils.fs.mkdirs("dbfs:/datasets/")
  dbutils.fs.cp(localpath, "dbfs:/datasets/")
    
  display(dbutils.fs.ls("dbfs:/datasets/" +  localFileName))  

  sqlContext.sql("drop table if exists " + tableName)
  var df = spark.read.option("header", "true") 
                        .option("inferSchema", "true")
                        .option("quote", "\"")
                        .option("escape", "\"")
                        .option("charset", "UTF-8")
                        .json("/datasets/" + localFileName)
   
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

// MAGIC %sql
// MAGIC select count(1) as `Num Records`, 'followers' as `Table` from twitter_followers
// MAGIC union
// MAGIC select count(1) as `Num Records`, 'tweets' as `Table` from twitter_timelines

// COMMAND ----------

// MAGIC %md
// MAGIC ## Laden des Codebook aus Google Spreadhsheets
// MAGIC ---
// MAGIC Der folgende Block lädt ein Codebuch mit 3 Spalten aus <a target="_blank" href="https://docs.google.com/spreadsheets/d/16oZlDQkr6kxOABy32Kj4nLN8VnNQMmW7jZEih_t1Mus/edit?usp=sharing">diesem Google Spreadsheet</a>.

// COMMAND ----------

import scala.sys.process._
// Choose a name for your resulting table in Databricks
var tableName = "codebook"

// Replace this URL with the one from your Google Spreadsheets
// Click on File --> Publish to the Web --> Option CSV and copy the URL
var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTNA0MbUnBI2-uKX3JbIJWWNYfv1VlRSDcamxUWCv-TspdKUhCJcN_QRI3AtWhh5P-C50BpUsxw7V4O/pub?output=csv"

var localpath = "/tmp/" + tableName + ".csv"
dbutils.fs.rm("file:" + localpath)
"wget -O " + localpath + " " + url !!

dbutils.fs.mkdirs("dbfs:/datasets/gsheets")
dbutils.fs.cp("file:" + localpath, "dbfs:/datasets/gsheets")

sqlContext.sql("drop table if exists " + tableName)
var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/datasets/gsheets/" + tableName + ".csv");
df.write.saveAsTable(tableName);

// COMMAND ----------

// MAGIC %md
// MAGIC Kurze Überprüfung der Daten in der Tabelle `codebook`:

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from codebook
// MAGIC --describe codebook

// COMMAND ----------

// MAGIC %md
// MAGIC ## Anwenden des Codebook auf die Tweets
// MAGIC ---
// MAGIC Nachdem das Codebook als Tabelle vorliegt können wir es auf die Tweets anwenden, um Themen zu identifzieren. Wir müssen dabei auf das Ergebnis aus der Textvorbereitung zurückgreifen, da wir eine Spalte mit jeweils einem Wort (Token) benötigen. Wir verwenden dazu den View `tweets_prep_step_4` aus dem Notebook "Texte für Analysen vorbereiten".

// COMMAND ----------

// MAGIC %sql
// MAGIC select user
// MAGIC       ,original_text
// MAGIC       ,word 
// MAGIC from tweets_prep_step_4

// COMMAND ----------

// MAGIC %md
// MAGIC ## Einfaches Anwenden des Codebuches ohne Aggregation
// MAGIC ---
// MAGIC Das unten stehende SQL verbindet die beiden Tabellen (Tweets / Codebuch) über das gesuchte Stichwort miteinander und gibt nur die Zeilen mit Treffern aus.

// COMMAND ----------

// MAGIC %sql
// MAGIC select user
// MAGIC       ,original_text
// MAGIC       ,word
// MAGIC       ,keyword
// MAGIC       ,topic
// MAGIC       ,weight
// MAGIC from tweets_prep_step_4 t
// MAGIC left join codebook c
// MAGIC   on t.word = c.keyword
// MAGIC where c.keyword is not null

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aggregieren der Gewichte als Summe
// MAGIC ---

// COMMAND ----------

// MAGIC %sql
// MAGIC select user
// MAGIC       ,original_text
// MAGIC       --,word
// MAGIC       --,keyword
// MAGIC       ,topic
// MAGIC       ,sum(weight) as `relevance`
// MAGIC       --,count(1) as `keyword_hits`
// MAGIC from tweets_prep_step_4 t
// MAGIC left join codebook c
// MAGIC   on t.word = c.keyword
// MAGIC where c.keyword is not null
// MAGIC group by user, original_text, topic
// MAGIC order by relevance desc

// COMMAND ----------

// MAGIC %md
// MAGIC ## Anwenden des Codebuches und Aggregation der Treffer
// MAGIC ---
// MAGIC Im obigen Beispiel wird ein Tweet mehrfach im Ergebnis auftauchen, wenn er mehrere der gesuchten Stichwörter enthält. Im Endergebnis wollen wir aber für jeden Tweet eine Zuordnung zu den Themen inklusive einer Zahl, die angibt, wie stark das Thema in diesem Tweet vertreten ist.

// COMMAND ----------

// MAGIC %sql
// MAGIC select user
// MAGIC       ,original_text
// MAGIC       ,collect_list(Struct(topic, relevance))
// MAGIC       ,max(relevance) as `max_relevance`
// MAGIC       ,keywords
// MAGIC from
// MAGIC (
// MAGIC   select user
// MAGIC         ,original_text
// MAGIC         ,topic
// MAGIC 
// MAGIC         -- Diese Funktion macht das Gegenteil von explode()
// MAGIC         ,collect_list(keyword) as keywords
// MAGIC 
// MAGIC         ,collect_list(ko) as ko
// MAGIC 
// MAGIC         ,sum(weight) as `relevance`
// MAGIC 
// MAGIC         -- Die Unterabfrage stellt sicher, dass wir keine doppelten Tweets haben die falsch aggregiert werden
// MAGIC   from (select distinct user, original_text, word from tweets_prep_step_4) t
// MAGIC   left join codebook c
// MAGIC     on t.word = c.keyword
// MAGIC   where c.keyword is not null
// MAGIC   group by user, original_text, topic
// MAGIC )
// MAGIC where array_contains(ko, 'y')
// MAGIC group by user, original_text, keywords
// MAGIC order by max_relevance desc
