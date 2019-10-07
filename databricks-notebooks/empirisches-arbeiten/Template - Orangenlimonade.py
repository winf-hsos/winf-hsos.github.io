# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # <span id="uebersicht">Übersicht</span>
# MAGIC ---
# MAGIC ## 1: Vorbereitung
# MAGIC ---
# MAGIC 1.1: Anlegen der Daten<br>
# MAGIC 1.2: Datenmodell<br>
# MAGIC 
# MAGIC ---
# MAGIC ## 2: Einfache Abfragen mit SQL
# MAGIC ---
# MAGIC 2.1: Spalten auswählen<br>
# MAGIC 2.2: Datensätze filtern<br>
# MAGIC 2.3: Zählen, Summieren und andere Aggregationen<br>
# MAGIC 2.4: Daten gruppieren<br>
# MAGIC 2.5: Das Ergebnis sortieren<br>
# MAGIC 
# MAGIC ---
# MAGIC ## 3: Erweiterte Abfragen mit SQL
# MAGIC ---
# MAGIC 3.1: Verhältnisgrößen berechnen mit SQL<br>
# MAGIC 3.2: Transformieren von Daten mit SQL<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;3.2.1: Mehrere Spalten zu einer Spalte zusammenfassen<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;3.2.2: Eine Spalte zu einer Zeile zusammenfassen<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;3.2.3: Anreichern der Daten mit weiteren Spalten<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; a) Regelbasierte Anreicherung<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; b) Verwendung von Mapping-Tabellen<br>
# MAGIC 
# MAGIC ---
# MAGIC ## 4: Statistische Analysen mit SQL
# MAGIC ---
# MAGIC 4.1: Lageparameter bestimmen<br>
# MAGIC 4.2: Zusammenhänge ermitteln<br>
# MAGIC 
# MAGIC ---
# MAGIC ## 5: Komplexe Abfragen mit SQL (nicht Teil dieser Einheit)
# MAGIC ---
# MAGIC 5.1: Abfragen über mehrere Tabellen (JOINs)<br>
# MAGIC 5.2: Mengenoperationen<br>
# MAGIC 5.3: Unterabfragen<br>
# MAGIC 5.4: Window-Funktionen<br>
# MAGIC 
# MAGIC ---
# MAGIC ## Links
# MAGIC ---
# MAGIC ### Fragebogen & Daten
# MAGIC [🔗 Datenmodell Orangenlimonade als XLS](https://docs.google.com/spreadsheets/d/1Sq_CWA-oTN90d0EpA6rEDB3C77dyIXv0HZTq3YWuyy8/edit?usp=sharing)<br>
# MAGIC [🔗 Fragebogen Orangenlimonade inklusive Codierung (PDF)](https://drive.google.com/file/d/1-8aEkgxhgUPgRanEoxIu_2vQQybpLeyO/view?usp=sharing)<br>
# MAGIC 
# MAGIC ### SQL
# MAGIC [🔗 SQL Cheat Sheet](https://docs.google.com/presentation/d/1jgeZ3RKLmgDgiES1UpvQwUKYGGBWEmpNpYXFVEQIh6s/export/pdf)<br>
# MAGIC [🔗 Slides zu SQL aus der Veranstaltung "Information Management"](https://docs.google.com/presentation/d/1Ga31SJKo6KTfMq0m2Z5T7eTmGqMPdBn5cLVnzWHWS4k/export/pdf)<br>
# MAGIC [🔗 Spark SQL Funktionsreferenz](https://spark.apache.org/docs/latest/api/sql/)<br>

# COMMAND ----------

# MAGIC %md
# MAGIC # 1: Vorbereitung
# MAGIC 
# MAGIC Bevor wir loslegen können, müssen wir die Daten in eure persönlichern Databricks-Accounts laden und uns mit dem Datenmodell vertraut machen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1: Anlegen der Daten
# MAGIC ---
# MAGIC Den Block unten müsset ihr nur einmal ausführen. Der Code legt die benötigte Tabelle `limonade` an, mit der wir im Folgenden weiter arbeiten werden.

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.sys.process._
# MAGIC val tables = Array("limonade")
# MAGIC 
# MAGIC var delimiter = ",";
# MAGIC 
# MAGIC for(t <- tables) {
# MAGIC 
# MAGIC   val tableName = t
# MAGIC   dbutils.fs.rm("file:/tmp/" + tableName + ".csv.gz")
# MAGIC 
# MAGIC   "wget -P /tmp https://s3.amazonaws.com/nicolas.meseth/data+sets/" + tableName + ".csv.gz" !!
# MAGIC 
# MAGIC   val localpath = "file:/tmp/" + tableName + ".csv.gz"
# MAGIC   dbutils.fs.rm("dbfs:/datasets/" + tableName + ".csv.gz")
# MAGIC   dbutils.fs.mkdirs("dbfs:/datasets/")
# MAGIC   dbutils.fs.cp(localpath, "dbfs:/datasets/")
# MAGIC   display(dbutils.fs.ls("dbfs:/datasets/" +  tableName + ".csv.gz"))
# MAGIC 
# MAGIC   sqlContext.sql("drop table if exists " + tableName)
# MAGIC   val df = spark.read.option("header", "true") 
# MAGIC                         .option("inferSchema", "true")
# MAGIC                         .option("sep", delimiter)
# MAGIC                         .option("quote", "\"")
# MAGIC                         .option("escape", "\"")
# MAGIC                         .csv("/datasets/" +  tableName + ".csv.gz")
# MAGIC   df.unpersist()
# MAGIC   df.cache()
# MAGIC   df.write.saveAsTable(tableName);  
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2: Datenmodell
# MAGIC 
# MAGIC Eine Liste mit allen Spalten könnt ihr [hier als Spreadsheet](https://docs.google.com/spreadsheets/d/1Sq_CWA-oTN90d0EpA6rEDB3C77dyIXv0HZTq3YWuyy8/edit?usp=sharing) einsehen. Alternativ könnt ihr euch die Spalten einer Tabelle auch mithilfe des `describe` Befehls ausgeben lassen.
# MAGIC 
# MAGIC Den Fragebogen könnt ihr hier herunterladen (`Strg` gedrückt halten, um in einem neuen Tab zu öffnen): [Fragebogen inklusive Codierung (PDF)](https://drive.google.com/file/d/1-8aEkgxhgUPgRanEoxIu_2vQQybpLeyO/view?usp=sharing)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe limonade

# COMMAND ----------

# MAGIC %md
# MAGIC # 2: Einfache Abfragen mit SQL
# MAGIC SQL ist eine **Abfrage**sprache, die sich gut für strukturierte Daten eignet. Der Begriff "strukturiert" bedeutet in diesem Zusammenhang, dass wir die Daten in Tabellenform vorliegen haben, ähnlich wie in einem Excel-Spreadsheet. Konkret bedeutet das, dass die Daten in **Spalten** und **Zeilen** vorliegen. Jede Spalte besitzt einen Namen und speichert ein Merkmal bezüglich der Daten. Die Daten liegen zeilenweise vor, also z.B. repräsentiert jede Zeile in der Tabelle einen ausgefüllten Fragebogen eines Teilnehmers.
# MAGIC 
# MAGIC Wir verwenden den SELECT-Befehl, um Daten abzufragen. Dabei können wir mit dem SELECT-Befehl unterschiedliche Methoden anwenden, um das Ergebnis nach unseren Wünschen zu gestalten. Einige dieser Methoden lernen wir anhand der folgenden Aufgabenstellungen kennen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1: Spalten auswählen
# MAGIC Eine wichtige Funktion des SELECT-Befehls ist die Einschränkung der Spalten, die im Ergebnis enthalten sind. Datensätze enthalten häufig sehr viele Spalten, der hier vorliegende Datensatz hat 274 (!) Spalten. Aber nur wenige sind für die Beantwortung einer Fragestellung tatsächlich relevant.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 1
# MAGIC ---
# MAGIC **Erstellt eine SQL-Abfrage, die im Ergebnis nur Informationen zum Studium des Teilnehmers beinhaltet!**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 2
# MAGIC ---
# MAGIC **Wählt aus den gesamten Spalten nur die Spalten für die Frage 12 ("Was verbinden Sie spontan mit dem Begriff Orangenlimonade?") aus!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2: Datensätze filtern
# MAGIC SQL lässt uns nicht nur bestimmen, welche Spalten wir im Ergebnis sehen wollen, sondern auch, welche **Zeilen**. Um das zu anzugeben, können wir so genannte Bedingungen definieren, die für jede Zeile gelten müssen. Gilt eine der Bedingungen nicht, so ist die Zeile nicht im Ergebnis enthalten.
# MAGIC 
# MAGIC Diese Filterbedingungen erstellen leiten wir mit dem Schlüsselwort `WHERE` ein, gefolgt von ein oder mehreren Bedingungen. Die folgenden Aufgaben bringen uns die WHERE-Klausel näher.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 3
# MAGIC ---
# MAGIC **Wir wollen uns nur die Antworten von Teilnehmern ansehen, deren Haushalt mindestens 2 Personen angehören. Filtert die Datensätze entsprechend!**
# MAGIC 
# MAGIC ✓ Die Größe des Haushalts wird in Frage 48 erfasst.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 4
# MAGIC ---
# MAGIC **Angenommen es interessieren uns nur Antworten von Teilnehmern, die 1990 oder später geboren wurden. Filtert die Datensätze entsprechend!**
# MAGIC 
# MAGIC ✓ Das Geburtsjahr wird in Frage 39 erfasst.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 5
# MAGIC ---
# MAGIC **Filtert die Daten so, dass nur Teilnehmer mit dem Status "Noch Schüler", "Volks-/ Hauptschulabschluss" und "Ohne Abschluss" enthalten sind!**
# MAGIC 
# MAGIC ✓ Der Schulabschluss wird in Frage 40 erfasst.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 6
# MAGIC ---
# MAGIC **Welche der männlichen Teilnehmer essen mehrmals pro Woche Fast-Food?**
# MAGIC 
# MAGIC ✓ Das Geschlecht wird in Frage 53 erfasst.<br>
# MAGIC ✓ Die Ernährungsgewohnheiten werden in Frage 38 erfasst.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3: Zählen, Summieren und andere Aggregationen
# MAGIC Häufig wollen wir aggregierte Werte aus den Daten ermitteln. Jedes Mal wenn wir fragen "*Wie viele...*" fragen wir nach einer Zahl, die letztlich das Ergebnis einer Aggregation der Daten ist. Es gibt viele andere Möglichkeiten, Daten zu aggregieren. Einige davon lernen wir anhand der folgenden Aufgaben kennen.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 7
# MAGIC ---
# MAGIC **Wie viele Antworten sind im Datensatz insgesamt enthalten?**
# MAGIC 
# MAGIC ✓ Die Funktion `count()` zählt Datensätze!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 8
# MAGIC ---
# MAGIC **Wie groß ist der größte Haushalt unter den Antworten?**
# MAGIC 
# MAGIC ✓ Die Größe des Haushalts wird in Frage 48 erfasst.<br>
# MAGIC ✓ `CAST(<col> AS DECIMAL) IS NOT NULL` prüft ob der Wert ind er Spalte einen numerischen Wert enthält<br>
# MAGIC ✓ Die Funktionen `max()` und `min()` sprechen für sich.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 9
# MAGIC ---
# MAGIC **Wie wichtig ist den Teilnehmern Orangenlimonade auf Parties im Durchschnitt?**
# MAGIC 
# MAGIC ✓ Diese Info findet ihr in Frage 25.<br>
# MAGIC ✓ Die Funktion `avg()` berechnet das arithmetische Mittel.<br>
# MAGIC ✓ Vorsicht, ihr müsst etwas beachten. Schaut euch mal das Ergebnis an und überlegt, ob es valide ist.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 10
# MAGIC ---
# MAGIC **Wie viele Personen leben in Summe in den Haushalten der Befragten?**
# MAGIC 
# MAGIC ✓ Diese Info findet ihr in Frage 48.<br>
# MAGIC ✓ Die Funktion `sum()` summiert numerische Daten.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 11
# MAGIC ---
# MAGIC **Aus welchem Geburtsjahrgang stammt der älteste Teilnehmer?**
# MAGIC 
# MAGIC ✓ Das Geburtsjahr wird in Frage 39 erhoben.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4: Daten Gruppieren
# MAGIC 
# MAGIC Wir haben oben Aggregationsfunktionen kennengelernt, die uns jeweils einen Wert zurückliefern. Beispiele waren die Summe, der Durchschnitt oder der größte Wert der gesamten Daten. Oft reicht aber ein Wert alleine nicht aus, sondern wir brauchen je einen Durchchnittswert pro irgendeiner Untergruppe. Beispielsweise das Durschnittsalter pro Geschlecht.
# MAGIC 
# MAGIC Um das mit SQL zu erreichen nutzen wir so genannte Gruppierungen, die wir auf Basis von Spalten (oder Ausdrücken) bilden können. Wir verwenden hierfür das Schlüsselwort `group by` und nennen dann die zu gruppierenden Spalten (oder Ausdrücke).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 12
# MAGIC ---
# MAGIC **Wie ist die Verteilung der Postleitzahlen unter den Antworten?**
# MAGIC 
# MAGIC ✓ Die PLZ wird in Frage 51 erhoben.<br>
# MAGIC ✓ Erstellt auch eine passende Visualisierung.<br>
# MAGIC ✓ Stellt sicher, dass nur gültige PLZ im Ergebnis enthalten sind.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 13
# MAGIC ---
# MAGIC **Wie ist die Verteilung der Geschlechter unter den Teilnehmern der Umfrage?**
# MAGIC 
# MAGIC ✓ Das Geschlecht wird in Frage 53 erhoben.<br>
# MAGIC ✓ Zeigt die Verteilung der absoluten Zahlen.<br>
# MAGIC ✓ Erstellt auch eine passende Visualisierung.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 14
# MAGIC ---
# MAGIC **Wie sind die Geburtsjahrgänge verteilt?**
# MAGIC 
# MAGIC ✓ Die Info findet ihr in Frage 39.<br>
# MAGIC ✓ Zeigt die Verteilung der absoluten Zahlen.<br>
# MAGIC ✓ Visualisiert das Ergebnis als Balkendiagramm, sortiert nach dem Jahrgang.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 15
# MAGIC ---
# MAGIC **Wie ist die Geschlechterverteilung pro Studiengang?**
# MAGIC 
# MAGIC ✓ Das Geschlecht wird in Frage 53 erhoben.<br>
# MAGIC ✓ Der Studiengang wird in Frage 44 erhoben.<br>
# MAGIC ✓ Zeigt die Verteilung der absoluten Zahlen.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5: Das Ergebnis sortieren
# MAGIC 
# MAGIC Oft ist es notwendig, das Resultat einer SQL Abfrage zu sortieren. Häufige Szenarien sind Top-Listen sowie die beste oder schlechteste Zeile im Ergebnis. Die folgenden Aufgaben helfen euch dabei, das Sortieren mit SQL anwenden zu lernen.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 16
# MAGIC ---
# MAGIC **Sortiert die Daten nach dem Alter der Befragten, so dass die Ältesten oben stehen!**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 17
# MAGIC ---
# MAGIC **Welche Einkommensklasse nennt im Durchschnitt den höchsten Preis für einen guten Deal (egal welche Limonadensorte)?**
# MAGIC 
# MAGIC ✓ Die Abfrage von Preisgrenzen erfolgt in Frage 21.<br>
# MAGIC ✓ Erstellt auch eine passende Visualisierung.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC # 3: Erweiterte Abfragen mit SQL
# MAGIC Der Funktionsumfang von SQL ist weitaus größer als wir ihn im vorigen Teil kennengelernt haben. Aber auch mit den einfachen Mitteln können wir schon sehr viel erreichen. Darum geht es in diesem Abschnitt.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1: Verhältnisgrößen berechnen mit SQL
# MAGIC 
# MAGIC Oft ist eine absolute Zahl nicht aussagekräftig, wenn z.B. die Verteilung der Grundgesamtheit ungleich ist. In diesem Fall sind relative Zahlen - oder Verhältniszahlen - eine sinnvolle Kenngröße. Oft verwendet man für relative Angaben die Prozentangabe (%).
# MAGIC 
# MAGIC In den folgenden Aufgaben versuchen wir, mithilfe von SQL relative Häufigkeiten in Prozentangabe zu berechnen.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aufgabe 18
# MAGIC ---
# MAGIC ** Wie ist die prozentuale Verteilung zwischen weiblichen und männlichen Teilnehmern?**
# MAGIC 
# MAGIC ✓ Die Informationen über das Geschlecht findet ihr in Frage 53.<br>
# MAGIC ✓ Eine Unterabfrage hilft euch dabei, die Gesamtzahl dynamisch zu ermitteln.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aufgabe 19
# MAGIC ---
# MAGIC ** Welcher Prozentsatz der Frauen und Männer ernährt sich "bewusst fleischarm"?**
# MAGIC 
# MAGIC ✓ Die Informationen bezüglich der Ernährungsweise findet ihr in Frage 47.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aufgabe 20
# MAGIC ---
# MAGIC ** Wie ist die prozentuale Verteilung der Geschlechter gruppiert nach höchstem Schulabschluss?**
# MAGIC 
# MAGIC ✓ Das Geschlecht wird in Frage 53 erhoben.<br>
# MAGIC ✓ Der Schulabschluss wird in Frage 40 erhoben.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2: Transformieren von Daten mit SQL
# MAGIC In vielen Fällen erhalten wir Rohdaten, die sich ohne eine vorgelagerte Verarbeitung nicht für die Analyse eignen. SQL erlaubt es uns, vielfältige Transformationen der Daten durchzuführen, und so das gewünschte Format zu erzeugen. Dabei kann man verschiedene Arten von Transformationen unterscheiden. Drei davon, die gerade im Umgang mit empirischen Daten aus Fragebögen relevant sind, lernen wir in den nächsten Aufgabenstellungen kennen.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2.1: Mehrere Spalten zu einer Spalte zusammenfassen
# MAGIC Mit SQL lassen sich mittels Ausdrücken neue Spalten erzeugen. Diese neuen Spalten können Daten aus vorhandenen Spalten verwenden, um neue Erkenntnisse zu gewinnen. Sie können aber auch volkommen unabhängig von den existierenden Spalten erzeugt werden. Anhand der folgenden Aufgaben werden wir unterschiedliche Wege kennenlernen, neue Spalten zu erstellen.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aufgabe 21
# MAGIC ---
# MAGIC ** Wie ist die Verteilung der Antworten aus Frage 1 ("Haben Sie in den letzten 12 Monaten Orangenlimonade gekauft oder getrunken?")?**
# MAGIC 
# MAGIC ✓ Überlegt, wie ihr die drei Antwortspalten in einer Spalte zusammenfassen könnt!<br>
# MAGIC ✓ Das `CASE WHEN` Statement kann euch helfen.<br>
# MAGIC ✓ Erstellt das Diagramm wie unten gezeigt!
# MAGIC 
# MAGIC ![](https://docs.google.com/drawings/d/e/2PACX-1vQO7oUqe5ysC4s9ZFCBfwhMNhrQyAmiosrRJqp1wA6pwtKwI585t37So5Ey5eUrhtHe2J5m5U7SeJnK/pub?w=958&h=448)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aufgabe 22
# MAGIC ---
# MAGIC **Wie viele _Liter_ Orangenlimonade kauft ein Teilnehmer durchschnittlich pro Woche?**
# MAGIC 
# MAGIC ✓ Nach der durchschnittlichen Menge wird in Frage 5 gefragt.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2.2: Eine Spalte zu einer Zeile zusammenfassen
# MAGIC Wir können mit SQL auch die Werte einer Spalte zu einer *Zeile* zusammenfassen. Das ist nützlich, wenn wir für unterschiedliche Antwortmöglichkeiten einer Frage jeweils eine Spalte im Datensatz haben. Anhand der folgenden Aufgabe werden wir unterschiedliche Wege kennenlernen, neue Spalten zu erstellen.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aufgabe 23
# MAGIC ---
# MAGIC ** Wie viele Teilnehmer kaufen in den in Frage 3 angegebenen Markttypen Orangenlimonade ein?**
# MAGIC 
# MAGIC ✓ Transformiert die Daten zunächst! In welcher Form braucht ihr die Daten?<br>
# MAGIC ✓ Wie geht ihr mit den "Sonstigen" Antworten um?<br>
# MAGIC ✓ Erstellt eine geeignete Visualisierung

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2.3: Anreichern der Daten mit weiteren Spalten
# MAGIC Manchmal ist es sinnvoll, die Daten mit neuen Spalten anzureichern. Z.B. um eine numerische Spalte mit einer textuellen Beschreibung zu ergänzen. Je nach Komplexität gibt es 2 Möglichkeiten, Spalten anzureichern:<br><br>
# MAGIC 
# MAGIC 1. Regelbasiert mit dem `CASE WHEN` Befehl
# MAGIC 2. Über Mappingtabellen
# MAGIC 
# MAGIC Beide Möglichkeiten wenden wir in den folgenden Aufgaben an.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aufgabe 24
# MAGIC ---
# MAGIC **Wie ist die Verteilung der Antworten bei Frage 4 ("Wie häufig konsumieren Sie Orangenlimonade?")**
# MAGIC 
# MAGIC ✓ Erstellt eine Visualisierung mit den textuellen Antwortmöglichkeiten in der Reihenfolge wie im Fragebogen!<br>
# MAGIC ✓ Zeigt auf der Y-Achse die absolute Häufigkeit für jede Antwortmöglichkeit dar!<br>
# MAGIC ✓ Reichert die textuelle Information mittels `CASE WHEN` Regeln an!<br>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aufgabe 25
# MAGIC ---
# MAGIC **Vergleicht die Angaben zu einem "angenehmen Preis" für jede der drei Limonadenmarken aus dem Fragebogen mit den tatsächlichen Preisen!**
# MAGIC 
# MAGIC ✓ Die Informationen zu den Preisangaben der Teilnehmer findet ihr in Frage 18.<br>
# MAGIC ✓ Die tatsächlichen Preise befinden sich in der Tabelle `limonade_prices`, die ihr als Mappingtabelle einbinden sollt.<br>
# MAGIC ✓ Im SQL Block unten wird diese Tabelle als temporärer View angelegt (einmal ausführen).<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anlegen der Mappingtabelle
# MAGIC create or replace temporary view limonade_prices as
# MAGIC select 'Fritz' as brand, 0.85 as price 
# MAGIC union
# MAGIC select 'Fanta' as brand, 0.59 as price
# MAGIC union  
# MAGIC select 'Vio' as brand, 0.75 as price

# COMMAND ----------

# MAGIC %md
# MAGIC # 4: Statistische Analysen mit SQL
# MAGIC Einfache statistische Analysen zählen auch zum Repertoire von SQL. Wir lernen in diesem Abschnitt ein paar kennen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1: Lageparameter bestimmen
# MAGIC Statistische Lageparameter geben einen Überblick über die Werteverteilung eines Merkmals. Gut bekannt sind das arithmetische Mittel oder der Median. Mittels SQL lassen sich die gängigsten Parameter auf einfache Weise bestimmen. In den folgenden Aufgaben wenden wir einige Funktionen zur Bestimmung von Lageparametern an.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 26
# MAGIC ---
# MAGIC ** Ermittelt die wichtigsten Lageparameter für die Zustimmung zur Aussage "Allgemein kann man sagen, dass ein höherer Preis auch eine höhere Qualität bedeutet"!**
# MAGIC 
# MAGIC ✓ Die Informationen zu dieser Aussage findet ihr in Frage 43.<br>
# MAGIC ✓ Was sind eurer Meinung nach gängige Lageparametr? Schaut in der [Funktionsreferenz](https://spark.apache.org/docs/latest/api/sql/) nach passenden SQL Funktionen!<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 27
# MAGIC ---
# MAGIC ** In Frage 12 wird nach den Begriffen gefragt, die spontan mit Orangenlimonade verbunden werden. Erstellt eine Tabelle, in der je eine Zeile pro abgefragten Begriff enthalten ist. In den Spalten sollen Lageparameter zur Wahrscheinlichkeit, dass der Begriff genannt wurde, enthalten sein.**
# MAGIC 
# MAGIC ✓ Errechnet im ersten Schritt die prozentualen Anteile nach Geschlecht.<br>
# MAGIC ✓ Überlegt welche weiteren Schritte notwendig sind.<br>
# MAGIC ✓ Die Infos werden in den Fragen 47 und 53 erfasst.<br>
# MAGIC ✓ Mithilfe von Variablen könnt ihr die SQL-Abfrage flexibel verwenden. Variablen könnt ihr mit `${name}` einbinden.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 28
# MAGIC ---
# MAGIC ** Stellt die wichtigsten Lageparameter für die 9 Beschreibungen bezüglich der Etiketten von Orangenlimonade aus Frage 28 grafisch dar!**
# MAGIC 
# MAGIC ✓ Welche Art der Darstellung ist geeignet?
# MAGIC ✓ Welche Datengrundlage benötigt ihr dafür?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2: Zusammenhänge ermitteln
# MAGIC Der Hauptgrund für die Datenanalyse ist die Suche nach Zusammenhängen. Welche Merkmale wirken sich in welcher Weise aufeinander aus? SQL ermöglicht die Schaffung der Datenbasis, um Hypothesen über Zusammenhänge mit faktenbasiert zu überprüfen. Die folgenden Aufgaben stellen implizit eine Hypothese auf, für die ihr in den Daten nach Belegen suchen sollt.
# MAGIC 
# MAGIC Bei Zusammenhängen ergibt eine Visualisierung fast immer Sinn. Überlegt euch für jede Aufgabe, welche am besten geeignet ist. Überlegt euch auch, welche Kennzahlen ihr errechnen könnt, um den Zusammenhang statistisch zu untermauern.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 29
# MAGIC ---
# MAGIC ** Gibt es einen Zusammenhang zwischen dem Geschlecht und dem Verfolgen einer bestimmten Ernährungweise?**
# MAGIC 
# MAGIC ✓ Errechnet im ersten Schritt die prozentualen Anteile nach Geschlecht.<br>
# MAGIC ✓ Überlegt welche weiteren Schritte notwendig sind.<br>
# MAGIC ✓ Die Infos werden in den Fragen 47 und 53 erfasst.<br>
# MAGIC ✓ Mithilfe von Variablen könnt ihr die SQL-Abfrage flexibel verwenden. Variablen könnt ihr mit `${name}` einbinden.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 30
# MAGIC ---
# MAGIC ** Essen Männer mit einer höheren Wahrscheinlichkeit mehrmals pro Woche Fast-Food als Frauen? **
# MAGIC 
# MAGIC ✓ Das Geschlecht wird in Frage 53 erfasst.<br>
# MAGIC ✓ Die Ernährungsgewohnheiten werden in Frage 38 erfasst.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufgabe 31
# MAGIC ---
# MAGIC ** Könnt ihr einen Zusammenhang zwischen dem Haushaltseinkommen und dem Alter anhand der Daten feststellen?**
# MAGIC 
# MAGIC ✓ Das Haushaltseinkommen wird in Frage 52 erhoben.<br>
# MAGIC ✓ Das Geburtsjahr wird in Frage 39 erhoben.
