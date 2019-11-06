-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Template - Campusbier 1.0 - Vorbefragung
-- MAGIC ## Vorbereitung: Daten einmalig anlegen
-- MAGIC ---
-- MAGIC Den Block unten müsst ihr einmalig ausführen, um die Daten in eurem Databricks-Account anzulegen. Danach könnt ihr Abfragen auf der neuen Tabelle `pre_survey` durchführen.
-- MAGIC 
-- MAGIC Um die Fragen zu beantworten ist es sinnvoll, den Fragebogen zu kennen:
-- MAGIC 
-- MAGIC <a href="https://drive.google.com/file/d/1APvF0EgV-fQR9waBPYZcO7ZoO3gv-fV4/view?usp=sharing" target="_blank">Link zum Fragebogen (öffnet sich in neuen Fenster)</a>

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC import org.apache.spark.sql.types.{TimestampType}
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "pre_survey"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT_lPJ9rUjtqoXcDsTDneHaBJteVbEIjtRfn-RFNJOYKEO4JzPZJZ8JwCzxVLLQA8v08M7KXXaQehqD/pub?gid=0&single=true&output=csv"
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
-- MAGIC df = df.withColumn("submitdate", unix_timestamp($"submitdate", "MM/dd/yyyy HH:mm:ss").cast(TimestampType))
-- MAGIC df = df.withColumn("startdate", unix_timestamp($"startdate", "MM/dd/yyyy HH:mm:ss").cast(TimestampType))
-- MAGIC df = df.withColumn("datestamp", unix_timestamp($"datestamp", "MM/dd/yyyy HH:mm:ss").cast(TimestampType))
-- MAGIC 
-- MAGIC df.write.saveAsTable(tableName);

-- COMMAND ----------

-- Zeigt die Metadatan der Tabelle an
describe pre_survey

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fragen an die Daten
-- MAGIC ---
-- MAGIC Versucht die folgenden Fragen mit SQL zu lösen. Stellt die Ergebnisse wenn sinnvoll auch visuell dar.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1: Wann startete die Umfrage und wann endete sie?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2: Wie viele Fragebögen wurden an jedem Tag aufgefüllt?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3: Welcher Anteil der Teilnehmer*innen hat das Bier bereits getrunken?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4: Welches ist das beliebteste Gebinde auf Basis der Antworten?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5: Gibt es einen Unterschied in der Präferenz für Craft-Beere zwischen Käufer*innen, die bei Discountern Bier kaufen, und anderen?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6: Welchen Preis sollte das Campusbier-Team auf Basis der Umfrage für einen 6er-Träger verlangen?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7: Wie ist das Durchschnittsalter der Teilnehmer*innen? Berechnet auch den Median!

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8: Wie groß ist der Anteil der Teilnehmer*innen, die bereit sind für ein wirklich gutes Bier, deutlich mehr Geld auszugeben?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9: Wie ist die Verteilung der Postleitzahlen?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10: Wertet die Angaben zu den weiteren Tipps der Teilnehmer*innen systematisch aus!

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage
