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

// MAGIC %md
// MAGIC # Texte für die Analyse mit SQL vorbereiten
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schritt 1: Zeilen filtern
// MAGIC ---

// COMMAND ----------

// MAGIC %sql
// MAGIC -- View enthält im Ergebnis nur Tweets mit dem Hashtag #organic
// MAGIC create or replace view tweets_prep_step_1 as
// MAGIC select id
// MAGIC       ,user
// MAGIC       ,text
// MAGIC       ,created_at
// MAGIC from twitter_timelines
// MAGIC where array_contains(hashtags, 'organic')

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tweets_prep_step_1

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schritt 2: Texte säubern und normalisieren
// MAGIC ---
// MAGIC 
// MAGIC - Sonderzeichen entfernen
// MAGIC - Satzzeichen entfernen
// MAGIC - In Kleinbuchstaben umwandeln
// MAGIC - 2 oder mehr Leerzeichen durch eins ersetzen

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view tweets_prep_step_2 as
// MAGIC select id 
// MAGIC       ,user
// MAGIC       ,text as original_text
// MAGIC       ,created_at
// MAGIC       -- Remove two or more subsequent white spaces
// MAGIC       ,regexp_replace(
// MAGIC         -- Remove special characters
// MAGIC         regexp_replace(
// MAGIC           -- Make all text lower case
// MAGIC           lower(
// MAGIC             -- Replace line breaks (2 different types)
// MAGIC             regexp_replace(
// MAGIC               regexp_replace(text, '\n', ' '), '\r', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ') as `text`
// MAGIC from tweets_prep_step_1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tweets_prep_step_2

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Hashtags entfernen (optional)
// MAGIC ---
// MAGIC Sonderfall bei Tweets. Muss vor dem Ersetzen von Sonderzeichen erfolgen, das sonst die # weg ist.

// COMMAND ----------

// MAGIC %sql
// MAGIC select regexp_replace(text, '#(\\w+)', ' ')
// MAGIC from tweets_prep_step_1

// COMMAND ----------

// MAGIC %md
// MAGIC ### URLs entfernen (optional)
// MAGIC ---
// MAGIC Sonderfall bei Texten mit URLS.

// COMMAND ----------

// MAGIC %sql
// MAGIC select regexp_replace(text, 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')
// MAGIC from tweets_prep_step_1

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view tweets_prep_step_2 as
// MAGIC select id
// MAGIC       ,user
// MAGIC       ,text as original_text
// MAGIC       ,created_at
// MAGIC       -- Remove two or more subsequent white spaces
// MAGIC       ,regexp_replace(
// MAGIC         -- Remove special characters
// MAGIC         regexp_replace(
// MAGIC           -- Make all text lower case
// MAGIC           lower(
// MAGIC             -- Replace URLs
// MAGIC             regexp_replace(
// MAGIC               -- Replace hashtags
// MAGIC               regexp_replace(
// MAGIC                 -- Replace line breaks (2 different types)
// MAGIC                 regexp_replace(
// MAGIC                   regexp_replace(text, '\n', ' '), '\r', ' '), '#(\\w+)', ' '), 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ') as `text`
// MAGIC from tweets_prep_step_1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tweets_prep_step_2

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schritt 3: In Wörter zerlegen (Tokenize)
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ### split() erzeugt einen Array

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view tweets_prep_step_3 as
// MAGIC   select id
// MAGIC         ,user
// MAGIC         ,created_at
// MAGIC         ,split(text, " ") as `words`
// MAGIC   from tweets_prep_step_2

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tweets_prep_step_3

// COMMAND ----------

// MAGIC %sql
// MAGIC select explode(words) as `word` from tweets_prep_step_3

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mit explode() kombinieren

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view tweets_prep_step_3 as
// MAGIC   select id
// MAGIC         ,user
// MAGIC         ,original_text
// MAGIC         ,created_at
// MAGIC         ,explode(split(text, " ")) as `word`
// MAGIC   from tweets_prep_step_2

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tweets_prep_step_3

// COMMAND ----------

// MAGIC %sql
// MAGIC select word, count(1)
// MAGIC from tweets_prep_step_3
// MAGIC where length(word) > 1
// MAGIC group by word
// MAGIC order by count(1) desc

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schritt 4: Stopwörter filtern
// MAGIC ---

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Naiver Ansatz
// MAGIC ---
// MAGIC Wir definieren manuell eine Liste an Stopwörtern und filtern diese raus.

// COMMAND ----------

// MAGIC %sql
// MAGIC select word, count(1) as `Anzahl`
// MAGIC from tweets_prep_step_3
// MAGIC -- Nicht sehr effizient
// MAGIC where word not in (select word from stopwords)
// MAGIC group by word
// MAGIC order by `Anzahl` desc

// COMMAND ----------

// MAGIC %md
// MAGIC ### Stopwords aus Google Spreadsheets laden
// MAGIC ---
// MAGIC Wir pflegen die Stopword-Liste in einem Google Spreadsheet, das wir veröffenltichen und dann hier mit dem unten stehenden Code-Block einlesen können.
// MAGIC 
// MAGIC **HINWEIS**: Beim allerersten Ausführen erscheint ein Fehler. Dann den Block erneut ausführen.

// COMMAND ----------

/*
 Wichtig: Das Google Spreadsheet muss vorher 
 veröffentlicht worden sein (Datei -> Im Web veröffentlichen)
*/
var tableName = "stopwords"
var sheetId = "1BtMOd_G9zaKqSGWXnswo4CnXB-qDyspTxnAAZw2DPhc"
var workbookNr = 1

import scala.util.parsing.json._
def get(url: String) = scala.io.Source.fromURL(url).mkString

// https://spreadsheets.google.com/feeds/list/1BtMOd_G9zaKqSGWXnswo4CnXB-qDyspTxnAAZw2DPhc/1/public/values?alt=json
var result = get("https://spreadsheets.google.com/feeds/list/" +  sheetId + "/" + workbookNr + "/public/values?alt=json")

result = result.replace("gsx$", "gsx_").replace("$", "")

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

scala.tools.nsc.io.File("/dbfs/datasets/spreadsheet_" + tableName + ".json").writeAll(result)

var dfStopwords = spark.read.json("/datasets/spreadsheet_" + tableName + ".json")

dfStopwords.createOrReplaceTempView("feed_" + tableName);

var stopwords = sqlContext.sql("select col.gsx_word.t as word from (select explode(feed.entry) from feed_" + tableName + ")")

sqlContext.sql("DROP TABLE IF EXISTS " + tableName);
stopwords.write.saveAsTable(tableName); 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from stopwords

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view tweets_prep_step_4 as
// MAGIC select id
// MAGIC       ,user
// MAGIC       ,original_text
// MAGIC       ,created_at
// MAGIC       ,word
// MAGIC from tweets_prep_step_3
// MAGIC where word not in (select word from stopwords)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schritt 5: POS Tagging (optional)
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ### POS-Tagging Tabelle überprüfen

// COMMAND ----------

// MAGIC %sql
// MAGIC select lower(word) as `word`, type from pos

// COMMAND ----------

// MAGIC %md
// MAGIC ### View für letzten Schritt definieren

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view tweets_prep_step_5 as
// MAGIC select t.word, p.type
// MAGIC from tweets_prep_step_4 t
// MAGIC left join pos p
// MAGIC   on p.word = t.word

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tweets_prep_step_5
