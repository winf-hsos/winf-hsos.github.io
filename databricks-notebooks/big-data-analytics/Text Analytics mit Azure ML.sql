-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Text Analytics mit Azure ML
-- MAGIC ---
-- MAGIC **Achtung:** Dieses Notebook geht davon aus, dass zuvor die Tabelle `twitter_timelines` geladen wurde.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exportieren der Tweets im benötigten Format
-- MAGIC ---
-- MAGIC Um die Tweets in das Big Data Helper Tool laden zu können, benötigen wir eine CSC-Datei mit den 3 Spalten `id`, `text` und `lang`. Alle 3 Spalten bekommen wir direkt aus der Tabelle `timelines`. Ihr könnt die unten stehende Abfrage anpassen, so dass nur die Tweets exportiert werden, die ihr als relevant identifiziert habt. 
-- MAGIC 
-- MAGIC **Bitte beachtet, dass die Anzahl der Tweets, die jede Gruppe mit der Azure Text Analytics API analysieren kann, begrenzt ist (Kostengründe)**

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Mit distinct gehen wir sicher, dass keine doppelten Tweets enthalten sind
-- MAGIC select distinct id, text, lang
-- MAGIC from twitter_timelines
-- MAGIC where 1=1 
-- MAGIC -- Hier die Filter entsprechend für die eigene Suche anpassen
-- MAGIC and array_contains(hashtags, 'organic')
-- MAGIC and array_contains(hashtags, 'pet')
-- MAGIC and lang in ('de', 'en')
-- MAGIC -- Die Menge im Ergebnis auf 500 Tweets begrenzen
-- MAGIC limit 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Hochladen der CSV-Datei im Big Data Analytics Helper Tool
-- MAGIC ---
-- MAGIC Das Tool zum Hochladen der Texte findet ihr hier: <a href="https://big-data-analytics-helper.glitch.me/text/index.html" target="_blank">Link zum Tool</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Laden der Ergebniss als Tabellen
-- MAGIC ---
-- MAGIC Nachdem ihr die CSV-Datei in das Big Data Analytics Helper Tool hochgeladen habt, werden die Ergebnisse des Azure Modells in 3 Teilen zurückgeliefert, die ihr mit dem folgenden Codeblock direkt als Tabellen in Databricks laden könnt. Ihr verfügt im Anschluss über die folgenden 3 neuen Tabellen:<br><br>
-- MAGIC 
-- MAGIC - `sentiment` - Die Stimmung in jedem analysierten Tweet
-- MAGIC - `keyPhrases` - Die Schlüsselbegriffe in jedem analysierten Tweet
-- MAGIC - `entities` - Erkannte Entitäten in jedem analysierten Tweet
-- MAGIC 
-- MAGIC **HINWEIS:** Der Gruppencode ist der selbe wie bei den Tweets / Followern.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("group_code", "", "Gruppencode")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC import org.apache.spark.sql.types.{TimestampType}
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC val tables = Array("sentiment", "keyPhrases", "entities")
-- MAGIC 
-- MAGIC val file_ending = ".json"
-- MAGIC val group_code = dbutils.widgets.get("group_code")
-- MAGIC 
-- MAGIC for(t <- tables) {
-- MAGIC 
-- MAGIC   val tableName = t
-- MAGIC   var localFileName = "azure%2Ftext%2F" + group_code + "%2Foutput_" + tableName + file_ending + "?alt=media"
-- MAGIC   val localpath = "file:/tmp/" + localFileName
-- MAGIC   dbutils.fs.rm("file:/tmp/" + localFileName)
-- MAGIC 
-- MAGIC   var url = "https://firebasestorage.googleapis.com/v0/b/big-data-analytics-helper.appspot.com/o/azure%2Ftext%2F" + group_code + "%2Foutput_" + tableName + file_ending + "?alt=media"  
-- MAGIC   
-- MAGIC   println(url);
-- MAGIC   
-- MAGIC   "wget -P /tmp " + url !!
-- MAGIC   
-- MAGIC   dbutils.fs.rm("dbfs:/datasets/" + tableName + file_ending)
-- MAGIC   dbutils.fs.mkdirs("dbfs:/datasets/")
-- MAGIC   dbutils.fs.cp(localpath, "dbfs:/datasets/")
-- MAGIC     
-- MAGIC   display(dbutils.fs.ls("dbfs:/datasets/" +  localFileName))  
-- MAGIC 
-- MAGIC   sqlContext.sql("drop table if exists " + tableName)
-- MAGIC   var df = spark.read.option("header", "true") 
-- MAGIC                         .option("inferSchema", "true")
-- MAGIC                         .option("charset", "UTF-8")
-- MAGIC                         .json("/datasets/" + localFileName)
-- MAGIC       
-- MAGIC   df.unpersist()
-- MAGIC   df.cache()
-- MAGIC   df.write.saveAsTable(tableName);  
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anwenden der Text-Analytics Ergebnisse auf die Tweets
-- MAGIC ---
-- MAGIC Das folgende SQL Statement führt die 3 Ergebnistabellen `sentiment`, `keyPhrases` und `entities`, die jeweils Teile der Antworten des ML-Modells enthalten, mit den Tweets zusammen.

-- COMMAND ----------

select t.id
      ,s.score
      ,k.keyPhrases
      ,t.text
      ,e.entities
from twitter_timelines t
left join (select distinct id, score from sentiment) s
  on t.id = s.id
left join (select distinct id, keyPhrases from keyPhrases) k
  on t.id = k.id
left join (select distinct id, * from entities) e
  on t.id = e.id
where s.id is not null
