-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Vorbereitung: Daten einmalig anlegen
-- MAGIC ---
-- MAGIC Den Block unten müsst ihr einmalig ausführen, um die Daten in eurem Databricks-Account anzulegen. Danach könnt ihr Abfragen auf der neuen Tabelle `beer_orders` durchführen und damit die Fragen aus der Fallstudie beantworten.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "beer_orders"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS5xqGRzI_inmoaHX-Dzq3aWCgDuo7DMHhfnY3FvnXsG1-V7RZuvW_l-9QJjhJ_eBb8tV2_gRiU290L/pub?gid=0&single=true&output=csv"
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
-- MAGIC df.write.saveAsTable(tableName);

-- COMMAND ----------

-- Zeigt die Metadatan der Tabelle an
describe beer_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.1: Wann war der Verkaufsstart und das -ende des ersten Projekts?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.2: Welche Produkte wurden im Shop angeboten?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.3: Wie viel Umsatz wurde insgesamt erzielt?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.4: Wie viel Umsatz wurde nur mit Bier erzielt?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.5: Wie ist der Umsatz über die einzelnen Tage der Verkaufsphase verteilt?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.6: Zu welchem netto Durchschnittspreis pro Flasche wurde das Haster Hell verkauft?
-- MAGIC 
-- MAGIC Hinweis: Der Preis enthält Pfand in Höhe von 3,92€ und 19% MwSt.

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.7: Wie viel Rabatt wurde durch die Verwendung von Rabattcodes insgesamt gewährt?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.8: Welcher Rabattcode wurde am häufigsten verwendet?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.9: Was fällt euch auf, wenn ihr die Verkaufspreise für das Haster Hell betrachtet?

-- COMMAND ----------

-- Deine Antwort als SQL-Abfrage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.10: Wie viele Tage vergehen im Durchschnitt zwischen der Bezahlung im Shop und dem Erhalt der Ware?
