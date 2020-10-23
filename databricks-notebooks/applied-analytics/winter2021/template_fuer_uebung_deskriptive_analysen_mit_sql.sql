-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Template für Übung: Deskriptive Analysen mit SQL
-- MAGIC ---
-- MAGIC Verwendet dieses Notebook-Template für die Beantwortung der Übungsaufgabe. Fügt eure SQL-Statements als neue Blöcke ein und strukturiert euer Notebook mit Markdown. Exportiert das Notebook anschließend für die Abgabe als HTML-Datei (File -> Export -> HTML).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1️⃣
-- MAGIC ---
-- MAGIC Erstellt eine SQL-Abfrage, die euch die **Anzahl täglicher Neuinfektionen** in den USA liefert. Erstellt geeignete Visualisierungen für die Verteilung der **Anzahl täglicher Neuinfektionen** seit dem 01.03.2020.

-- COMMAND ----------

-- Deine Lösung bitte hier eintragen (und diesen Kommentar entfernen)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2️⃣
-- MAGIC ---
-- MAGIC Ermittelt die 5 Länder mit den höchsten täglichen Neuinfektionsraten pro Millionen Einwohner der vergangenen Woche. Erstellt eine Visualisierung, die uns die Verteilung der **Anzahl täglicher Neuinfektionen pro Millionen Einwohner** seit dem 01.03.2020 für diese 5 Länder einfach miteinander vergleichen lässt. Die Visualisierung soll den Median darstellen und Ausreißer sichtbar machen.

-- COMMAND ----------

-- Deine Lösung bitte hier eintragen (und diesen Kommentar entfernen)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3️⃣
-- MAGIC ---
-- MAGIC Ermittelt für die 5 Länder aus der vorigen Aufgabe das **arithmetische Mittel** und den **Median** für die **Anzahl täglicher neuer Todesfälle** seit dem 01.03.2020. Erstellt eine Visualisierung, die die beiden Kenngrößen für jedes der Länder abbildet und schnell vergleichen lässt. Warum sind die Werte unterschiedlich und welche Kenngröße ist deiner Meinung nach besser geeignet?

-- COMMAND ----------

-- Deine Lösung bitte hier eintragen (und diesen Kommentar entfernen)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4️⃣
-- MAGIC ---
-- MAGIC Erstellt eine SQL-Abfrage, die den **Modus** für die Anzahl neuer Todesfälle seit dem 01.03.2020 in Deutschland ermittelt. Warum ist der Modus in diesem Fall vielleicht keine gute Kenngröße für die Beschreibung der Daten?

-- COMMAND ----------

-- Deine Lösung bitte hier eintragen (und diesen Kommentar entfernen)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5️⃣
-- MAGIC ---
-- MAGIC Ermittelt die **Spannweite** und die **Standardabweichung** der **täglichen Neuinfektionen** für jede Woche seit dem 01.03.2020 für Deutschland und seine Nachbarländer. Findet eine geeignete Visualisierung für die **Entwicklung der Standardabweichung für jedes Land im Zeitverlauf**.

-- COMMAND ----------

-- Deine Lösung bitte hier eintragen (und diesen Kommentar entfernen)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6️⃣
-- MAGIC ---
-- MAGIC Erstellt geeignete Visualisierungen, um mögliche Zusammenhänge zwischen im Datensatz befindlichen Merkmalen, wie z.B. des mittleren Alters der Bevölkerung, und der **Gesamtanzahl an Todesfällen** für alle Länder zu beschreiben. Erstellt pro Merkmal eine eigene Visualisierung.

-- COMMAND ----------

-- Deine Lösung bitte hier eintragen (und diesen Kommentar entfernen)

-- COMMAND ----------

select * from owid_covid
