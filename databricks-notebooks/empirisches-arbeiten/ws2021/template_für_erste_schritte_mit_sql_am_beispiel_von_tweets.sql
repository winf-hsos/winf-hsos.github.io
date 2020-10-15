-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Erste Schritte mit SQL am Beispiel von Tweets
-- MAGIC ---
-- MAGIC Dieses Notebook führt euch in die Grundlagen von SQL am Beispiel euren Tweets-Datensatzes ein. SQL ist eine universelle Abfrage- und Analysesprache für **strukturierte Daten** und kann auf jeden beliebigen Datensatz angewendet werden, dessen Daten in tabellarischer Form bestehend aus Zeilen und Spalten vorliegen.
-- MAGIC 
-- MAGIC Bitte beachtet, dass ihr zuerst eure Tweets in Databricks laden müsst. Dafür stelle ich euch ein separates Notebook bereit, in das ihr einfach euren API-Code eintragt und anschließend eure Daten laden könnt.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Was können wir mit SQL machen?
-- MAGIC ---
-- MAGIC SQL steht für *Structured Query Language* und ist eine Abfragesprache für strukturierte Daten. Damit meinen wir Daten in tabellarischer Form mit Zeilen und Spalten. Auf dieser Art von Daten können wir bestimmte grundlegende Abfrageoperationen ausführen, die wir für fast alle Analysen benötigen:<br><br>
-- MAGIC 
-- MAGIC 1. Spalten auswählen
-- MAGIC 2. Zeilen filtern
-- MAGIC 3. Zeilen sortieren
-- MAGIC 4. Zeilen aggregieren und gruppieren
-- MAGIC 
-- MAGIC Zu der Frage, was SQL ist und was wir damit machen können, habe ich auch einen Blogbeitrag auf Medium veröffentlicht: <a href="https://medium.com/@nicolas.meseth/what-is-sql-and-what-can-it-do-a2ad0204e47" target="_blank">What is SQL and what can we do with it?</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1. Spalten auswählen
-- MAGIC ---
-- MAGIC Ein Datensatz wie beispielsweise die Tweets kann viele Spalten haben. Für die meisten Fragestellungen benötigen wir jedoch nur einen kleinen Teil dieser Spalten. Daher brauchen wir eine Möglichkeit, nur die relevanten Spalten im Ergebnis zu behalten. Bevor wir uns anschauen, wie das mit SQL geht, lernen wir eine Möglicheit kennen, die Namen und Datentypen der Spalten im Datensatz auszugeben.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Spalten und deren Datentypen ausgeben

-- COMMAND ----------

-- Gibt alle Spalten der Tabelle aus
describe tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bestimmte Spalten auswählen
-- MAGIC ---
-- MAGIC Wir können mit SQL einfach die Spaltennamen mit Komma voneinander getrennt angeben, die wir im Ergebnis sehen wollen. Das SELECT-Statement beginnt immer mit dem Schlüsselwort `select`, gefolgt von den zu selektierenden Spalten oder Ausdrücken. Nach der Spaltenselektion erfolgt mit dem Schlüsselwort `from` die Angabe, aus welche Tabelle bzw. welchen Tabellen wir die Daten abfragen wollen.

-- COMMAND ----------

select
  screen_name,
  text
from
  tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Zeilen filtern
-- MAGIC ---
-- MAGIC Genauso wichtig wie das Auswählen der Spalten ist das Filtern von Zeilen. SQL erlaubt uns mit dem Schlüsselwort `where` Filterbedingungen zu definieren, die für jede Zeile im Datensatz geprüft wird. Nur Zeilen, bei denen die Bedingung wahr ist, blieben im Ergebnis enthalten.

-- COMMAND ----------

select
  screen_name,
  text
from
  tweets
where
  screen_name = 'kamo_de'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verknüpfte Bedingungen
-- MAGIC ---
-- MAGIC Im obigen Beispiel sind nur Zeilen im Ergebnis, bei denen die Spalte `screen_name` den Wert "kamo_de" (Prof. Karsten Morisse) aufweist. Wenn wir mehrere Bedingungen gleichzeitig anwenden wollen, dann können wir die Schlüsselwörter `and` oder `or` verwenden, um Bedingungen logisch miteinander zu verknüpfen.

-- COMMAND ----------

select
  screen_name,
  text
from
  tweets
where
  screen_name = 'kamo_de'
  and retweet_count > 1 -- Weitere Bedingung: Tweet muss öfter als 1 x retweeted worden sein

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Beliebige Ausdrücke
-- MAGIC ---
-- MAGIC Die Beispiele oben haben bereits die beiden arithmetischen Operatoren "=" und ">" eingeführt. Für weitere arithmetische Operationen gibt es analoge Symbole in SQL ("<", "<=", "<>", etc.).
-- MAGIC 
-- MAGIC Wir können in Bedingungen aber nicht nur arithmetische Operatoren verwenden, sondern auch beliebige Ausdrücke und Funktionen. Um einen Überblick über wichtige Funktionen in SQL zu erhalten ladet euch das von mir bereitgestellte <a href="https://docs.google.com/presentation/d/1jgeZ3RKLmgDgiES1UpvQwUKYGGBWEmpNpYXFVEQIh6s/export/pdf" target="_blank">SQL Cheat Sheet als PDF</a> herunter.
-- MAGIC 
-- MAGIC Im der Abfrage unten verwendet wir die Funktionen `year()`, um unser Ergebnis auf Tweets aus dem Jahr 2020 einzugrenzen.

-- COMMAND ----------

select
  created_at,
  screen_name,
  text
from
  tweets
where
  screen_name = 'kamo_de'
  and retweet_count > 1
  and year(created_at) = 2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Zeilen sortieren
-- MAGIC ---
-- MAGIC Das Sortieren von Daten ist im Kontext verschiedener Fragestellungen nützlich. Mit SQL können wir unter Vewendung des Schlüsselwortes `order by` unser Ergebnis nach beliebigen Spalten oder Ausdrücken sortieren.

-- COMMAND ----------

select
  screen_name,
  text,
  favorite_count
from
  tweets
where
  screen_name = 'kamo_de'
  and favorite_count > 0
order by
  favorite_count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Im obigen Beispiel sortieren wir die Tweets von Karsten Morisse **absteigend** nach der Anzahl der Likes. Die absteigende Sortierung erreichen wir durch das zusätzliche Schlüsselwort `desc`. Das Pendant für eine aufsteigende Sortierung ist `asc`. Ohne Angabe der expliziten Sortierreihenfolge sortiert SQL stets aufsteigend.
-- MAGIC 
-- MAGIC Auch für die Sortierung können mehrere Kriterien anwenden. Im Beispiel unten wird bei gleicher Anzahl Likes nach der Anzahl Retweets entschieden.

-- COMMAND ----------

select
  screen_name,
  text,
  favorite_count,
  retweet_count
from
  tweets
where
  screen_name = 'kamo_de'
  and favorite_count > 0
order by
  favorite_count desc,
  retweet_count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Das Ergebnis begrenzen
-- MAGIC ---
-- MAGIC Manchmal wollen wir nur die Top X Werte im Ergebnis haben. Mit SQL können wir mit `limit` das Ergebnis auf eine bestimmte Anzahl Zeilen beschränken.

-- COMMAND ----------

select
  screen_name,
  text,
  favorite_count
from
  tweets
where
  screen_name = 'kamo_de'
order by
  favorite_count desc
limit 10 -- Behalte nur die Top 10 im Ergebnis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Zeilen aggregieren und gruppieren
-- MAGIC ---
-- MAGIC Mit dem Filtern und Sortieren von Daten kann man rudimentäre Fragen beantworten. Für die meisten Fragestellungen benötigen wir jedoch die Möglichkeit, die Daten zusammenzufassen und zu verdichten. Auch das ist mit SQL möglich.
-- MAGIC 
-- MAGIC Wir schauen uns zunächst einfache Aggregationen von Daten an, bevor wir anschließend Beispiele für Aggregationen bei gleichzeitiger Bildung von Gruppen betrachten.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Einfache Aggregationen

-- COMMAND ----------

-- Wie viele Tweets hat ein Benutzer verfasst?
select
  count(*) as `Anzahl Tweets`
from
  tweets
where
  screen_name = 'kamo_de'

-- COMMAND ----------

-- Wie viele Likes haben die Tweets eines Benutzers im Durchschnitt erhalten?
select
  avg(favorite_count) as `Durchschnittliche Anzahl Likes`
from
  tweets
where
  screen_name = 'kamo_de'

-- COMMAND ----------

-- Was war die größte Anzahl Retweets eines Tweets des Benutzers?
select
  max(retweet_count) as `Größte Anzahl Retweets`
from
  tweets
where
  screen_name = 'kamo_de'
  and is_retweet = false -- Nur originäre Tweets des Users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregationen und Gruppierung
-- MAGIC ---
-- MAGIC Die obigen Beispiele liefern genau eine Zahl als Ergebnis zurück. In vielen Fällen wollen wir aber die gleiche Zahl für unterschiedliche Gruppen in den Daten ermitteln. Hier hilft uns das Schlüsselwort `group by`, gefolgt von Spaltennamen oder Ausdrücken, nach denen gruppiert werden soll.

-- COMMAND ----------

-- Wie viele Likes haben die Tweets aller Benutzer im Durchschnitt erhalten? Wer schneidet am besten ab?
select
  screen_name,
  avg(favorite_count) as `Durchschnittliche Anzahl Likes`
from
  tweets
group by
  screen_name
order by `Durchschnittliche Anzahl Likes` desc
limit 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Wie auch beim Filtern und Sortieren können wir auch beim Gruppieren mehrere Spalten oder Ausdrücke angeben und so verschachtelte Gruppen bilden.

-- COMMAND ----------

-- Wie viele Tweets gab es pro Monat und Jahr?
select
  year(created_at) as `Jahr`,
  month(created_at) as `Monat`,
  count(*) as `Anzahl Tweets`
from
  tweets
where
  year(created_at) >= 2020
group by
  year(created_at),
  month(created_at)
order by `Monat`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filtern auf gruppierten Daten
-- MAGIC ---
-- MAGIC Mit dem Schlüsselwort `where` können wir zeilenweise filtern,. Diese Filterung passiert vor der Anwendung der Aggregationen und Gruppierungen. Wenn wir Filterungen aus Basis der gruppierten Werte vornehmen wollen können wir das Schlüsselwort `having` verwenden.

-- COMMAND ----------

-- Wie viele Likes haben die Tweets aller Benutzer im Durchschnitt erhalten? Wer schneidet am besten ab?
select
  screen_name,
  avg(favorite_count) as `Durchschnittliche Anzahl Likes`
from
  tweets
group by
  screen_name
having
  `Durchschnittliche Anzahl Likes` > 20 -- Nur Gruppen mit durchschnittlich > 20 Like beibehalten
order by
  `Durchschnittliche Anzahl Likes` desc
