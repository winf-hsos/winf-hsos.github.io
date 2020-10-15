-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Notebook zum Laden eurer Tweets
-- MAGIC ---
-- MAGIC Dieses Notebook könnt ihr verwenden, um euren Tweets-Datensatz in euren Databricks-Account zu importieren. Über das <a href="https://big-data-analytics-helper.web.app" target="_blank">Twitter Collector Tool (TCT)</a> habt ihr die Möglichkeit, Twitter-User zu eurem Datensatz hinzuzufügen. Das Tool sammelt daraufhin für jeden eingetragenen Twitter-User sämtliche Tweets und aktualisiert diese täglich mehrmals. Wenn ihr die Daten in Databricks analysieren wollt klickt ihr auf den "Export"-Knopf. Nach Abschluss des Exports könnt ihr mit Hilfe dieses Notebooks die Daten in euren Databricks-Account laden und mit SQL analysieren. Diese Schritte könnt ihr wiederholen, z.B. wenn ihr eine Woche später die aktuellesten Tweets analysieren wollt.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Das Eingabefeld für den API Code erstellen
-- MAGIC ---
-- MAGIC Der folgende Code-Block erstellt das Eingabefeld oben links im Notebook. Hier könnt ihr den API Code eurer Gruppe eintragen. Dieser Code wird verwendet, um im Block darunter den richtigen (euren) Datensatz zu laden.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("api_key", "", "API Code")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Die Tweets aus eurem Datensatz importieren
-- MAGIC ---
-- MAGIC Der folgende Code-Block lädt unter Verwendung des eingegebenen API-Codes den entsprechenden Datensatz eurer Gruppe. Ihr könnt diesen Block erneut ausführen, um eine aktuelle Version eurer Daten zu erhalten. Denkt daran, dass ihr dafür vorher im [Twitter Collector Tool (TCT)](https://big-data-analytics-helper.web.app/) den Export-Knopf drücken müsst. Das Tool sammelt im Hintergrund täglich die neusten Tweets eurer Benutzer automatisch, ihr müsst sie aber manuell mittels des Knopfes exportieren und anschließend hier laden.
-- MAGIC 
-- MAGIC Nachdem der Code-Block durchgelaufen ist (kann ca. 1-2 Min. dauern) und die Daten geladen sind, solltet ihr eine Tabelle `tweets` in eurem Account haben. Diese Tabelle werden wir im Folgenden mit SQL abfragen und unterschiedliche Analysen auf den Daten durchführen.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC import org.apache.spark.sql.types.{TimestampType}
-- MAGIC import org.apache.spark.sql.types.{IntegerType}
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC spark.conf.set("spark.sql.session.timeZone", "GMT+2")
-- MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
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
-- MAGIC # Daten überprüfen
-- MAGIC ---
-- MAGIC Nach dem erneuten Laden ist es sinnvoll sich zu vergewissern, onb die neuen Daten nun auch wirklich vorhanden sind. Dafür gibt es verschiedene Möglichkeiten. Wir können uns z.B. den neuesten Zeitstempel im Datensatz ausgeben, oder einfach die Anzahl Tweets zählen und mit dem vorigen Stand vergleichen. Für beide Vorgehen seht ihr unten die entsprechenden SQL-Statements.

-- COMMAND ----------

-- Diese Abfrage gibt das größte Datum unter allen Tweets zurück
select
  max(created_at) as `Datum des neusten Tweets`
from
  tweets

-- COMMAND ----------

-- Diese Abfrage gibt die Anzahl an Tweets in eurem Datensatz zurück
select
  count(*) as `Anzahl Tweets gesamt`
from
  tweets
