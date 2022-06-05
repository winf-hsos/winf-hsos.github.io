# Databricks notebook source
# MAGIC %md
# MAGIC # Rahmen

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open Food Facts Datenbank 🥗
# MAGIC Der Fallstudie für das Sommersemester 2022 liegt der Datensatz der Plattform <a href="https://de.openfoodfacts.org/" target="_blank">Open Food Facts</a> zugrunde. Die Plattform erlaubt es jedem, Daten zu Lebensmittelprodukten zu teilen und so kollektiv eine Wissensbasis für mehr Transparenz im Lebensmittelbereich zu schaffen. Beispiele für typische Informationen sind die Herkunft, die Nährwertangaben, Zutaten, Angaben zu Allergen, Zusatzstoffen, vergebenen Labels oder dem Nutri-Score.
# MAGIC 
# MAGIC Die Datenbank wird auch in der Wissenschaft für Analysen herangezogen. Ein Beispiel findet sich in dem Paper <a href="https://pubmed.ncbi.nlm.nih.gov/34444941/" target="_blank">"Two Dimensions of Nutritional Value: Nutri-Score and NOVA"</a>, in dem versucht wird, die beiden Kennzahlen zu vergleichen und auf Schnittmengen hin zu überprüfen. Im dritten Teil der Fallstudie sollt ihr versuchen, einige Analysen des Papers selbst durchzuführen. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Hinweise zur Bearbeitung

# COMMAND ----------

# MAGIC %md
# MAGIC ## Erlaubte Werkzeuge 🛠
# MAGIC ---
# MAGIC - Für die Bearbeitung der Fallstudie dürft ihr **Databricks-Notebooks** oder das **RStudio und R-Skripte** verwenden.
# MAGIC - Neben **R und dem Tidyverse** dürft ihr auch **SQL als Abfragesprache** verwenden.
# MAGIC - Die Verwendung von Python oder Scala ist in begründeten Ausnahmen auch möglich. Das wäre z. B. der Fall, wenn ihr eine fortgeschrittene Methode über die Inhalte des Kurses hinaus anwenden wollt, die ihr  mit den im Kurs erlernten Mitteln nicht umsetzen könntet. Für solche Ausnahmen müsst ihr euch bei mir bitte ein kurzes OK einholen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisieren 📊
# MAGIC ---
# MAGIC - Erstellt *immer* mindestens eine Visualisierung für euer Ergebnis, es sei denn, es wird explizit nicht gefordert. 
# MAGIC - Überlegt, welche Visualisierungform(en) geeignet ist/sind. Erstellt die Visualisierungen entweder mit `ggplot2` oder mit den Bordmitteln der Databricks-Notebooks. 
# MAGIC - Für einige Aufgaben sollte ihr publizierfähige Visualisierungen erstellen. Hier reichen die Bordmittel der Databricks-Notebooks nicht aus. Für diese Aufgaben wird explizit eine Visualisierung mit `ggplot2` gefordert, bei denen ihr auch auf die kleinen Details achten müsst.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strukturieren & Kommentieren 💬
# MAGIC ---
# MAGIC - Oft wird eure Lösung zu einer Frage aus mehreren Schritten bestehen. Bitte stellt sicher, dass ihr im ersten Block **zunächst den Lösungsweg beschreibt** (in einem Textblock mit Markdown oder einem mehrzeiligen Kommentar in RStudio). Anschließend folgt dann die Umsetzung der beschriebenen Blöcke/Anweisungen in Form von Code (R oder SQL, in Ausnahmen auch Python oder Scala).
# MAGIC - Neben dem R oder SQL-Code sollt ihr zu jeder Aufgabe auch einen **Kommentar mit einer kurzen Ergebnisinterpretation** schreiben. In Databricks-Notebooks verwendet ihr dafür Markdown-Blöcke (`%md`). In R-Skripte könnt ihr Kommentare mit dem Raute-Symbol einfügen (`#`).
# MAGIC - Bitte **kommentiert euren Code so, dass er einfach lesbar und verständlich ist**. Nutzt dazu Kommentare direkt im Code oder schiebt Textblöcke mit Markdown ein, um komplexere Schritte zu erläutern. Eine unbeteiligte Person sollte euren Lösungsweg (Vorgehen und technische Umsetzung) gut nachvollziehen können.
# MAGIC - Auch Visualisierungen bedürfen eines Kommentars. Schreibt **unter jede Visualisierung daher eine kurze Interpretation**.
# MAGIC - Es ist so gewollt, dass manche Fragen auf unterschiedliche Weise interpretiert werden können. Trefft in diesem Fall Annahmen, unter der ihr die Frage beantwortet. **Dokumentiert eure Annahmen** zu Beginn zusammen mit der Darlegung des Lösungsweges.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Umfang 🥵
# MAGIC ---
# MAGIC - Je nach Vorkenntnissen und Intensität der Vorbereitung kann die die Fallstudie stellenweise schwierig und dadurch umfangreich sein. Ihr müsst für das Bestehen nicht zu jeder Aufgabe eine (gute) Lösung finden. 
# MAGIC - Wenn ihr nicht nur bestehen wollt, nutzt die Zeit ab der Veröffentlichung bis zur nächsten Sitzung, um für die Fragen mögliche Lösungswege zu durchdenken. Nur so könnt ihr entsprechende Fragen im Forum formulieren. Für die Besprechung der Fragen ist in der Sitzung ein Großteil der Zeit reserviert. Außerhalb der Sitzung steht euch auch das Formum für Fragen zur Verfügung, allerdings kann ich keine Antwortzeit garantieren (auch wenn ich bemühe).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Form der Abgabe 📨
# MAGIC ---
# MAGIC - Jede Gruppe gibt ihre Lösung der Fallstudie als ZIP-Datei ab. In dieser ZIP-Datei dürfen folgende Dateien enthalten sein:
# MAGIC   - Exportierte Databricks-Notebooks im HTML-Format (File → Export → HTML).
# MAGIC   - R-Skripte mit der Endung `.R`, die ihr z. B. mit dem RStudio erstellt habt.
# MAGIC   - Externe Tabellen im CSV oder XLSX-Format.
# MAGIC - Die ZIP-Datei muss vor Ablauf der Frist über die Abgabefunktion in ILIAS hochgeladen werden. Genaue Informationen dazu folgen in ILIAS.
# MAGIC - **Jedes Skript und Notebook muss von oben nach unten ausführbar sein**. Konkret bedeutet das, dass eine dritte Person (wie ich bei der Begutachtung) die Code-Blöcke oder Anweisungen hintereinander ohne Fehler und weitere Abhängigkeiten ausführen können muss. Solltet ihr weitere Tabellen einbinden, wie z. B. eine selbst erstellte XLSX-Datei, muss diese im ZIP-Archiv mitgeliefert werden. Bei R-Skripten sollte das Skript die Datei im selben Arbeitsverzeichnis erwarten (keine Pfadangaben beim Laden). Bei Databricks-Notebooks muss das Notebook ein Ladeskript beinhalten, das die Datei aus dem Standard-Uploadordner im Databricks File System (DBFS) lädt. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Daten 💾
# MAGIC Den Datensatz stelle ich euch als komprimierte Datei im JSON-Format bereit. Im komprimierten Zustand ist der Datensatz ca. 665 MB groß. Im entpackten Zustand sind es ca. 6,2 GB. Die Daten habe ich im März 2022 von der Open Food Facts Webseite heruntergeladen. Die Daten sind unverarbeitet, ich habe lediglich die Datenmenge reduziert, indem ich unwichtige, oft technische, Felder entfernt habe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initiales Laden der Daten (nur einmal ausführen ⚠)
# MAGIC Das Skript unten kopiert den Datensatz in euren Databricks-Account und erstellt anschließend die Tabelle `food_facts` daraus. **Bitte führt dieses Skript nur einmal aus**. Wenn ihr später ein neues Cluster erstellt habt, nutzt bitte den Code im Block weiter unten, um die Tabelle aus den vorhandenen Daten wiederherzustellen. So müsst ihr den Datensatz nicht erneut kopieren, was sehr lange dauert.

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.sys.process._
# MAGIC import org.apache.spark.sql.types.{TimestampType}
# MAGIC import org.apache.spark.sql.types.{IntegerType}
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC spark.conf.set("spark.sql.session.timeZone", "GMT+2")
# MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
# MAGIC 
# MAGIC val table_name = "food_facts"
# MAGIC val file_name = "open_food_products_germany.json.gz"
# MAGIC 
# MAGIC val localpath = "file:/tmp/" + file_name
# MAGIC dbutils.fs.rm(localpath)
# MAGIC 
# MAGIC var url = "https://s3.amazonaws.com/nicolas.meseth/data-sets/" + file_name
# MAGIC "wget " + url + " -O /tmp/" +  file_name !!
# MAGIC 
# MAGIC dbutils.fs.rm("dbfs:/datasets/" + file_name)
# MAGIC dbutils.fs.mkdirs("dbfs:/datasets/")
# MAGIC dbutils.fs.cp(localpath, "dbfs:/datasets/")
# MAGIC 
# MAGIC display(dbutils.fs.ls("dbfs:/datasets/" +  file_name))  
# MAGIC 
# MAGIC sqlContext.sql("drop table if exists " + table_name)
# MAGIC var df = spark.read.option("inferSchema", "true")
# MAGIC                    .option("quote", "\"")
# MAGIC                    .option("escape", "\\")
# MAGIC                    .json("/datasets/" + file_name)
# MAGIC 
# MAGIC df.write.saveAsTable(table_name);
# MAGIC 
# MAGIC // Clean up
# MAGIC dbutils.fs.rm("dbfs:/datasets/" + file_name)
# MAGIC dbutils.fs.rm("file:/tmp/" + file_name)
# MAGIC 
# MAGIC // Show result summary
# MAGIC display(sqlContext.sql("""
# MAGIC     select 'food_facts', count(1) from food_facts
# MAGIC     """).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wiederherstellen der Tabelle (nach dem initialen Laden verwenden ✔)
# MAGIC Das SQL-Skript unten stellt die Tabelle nach jedem Erzeugen eines neuen Clusters aus den vorhandenen Daten wieder her. Das sollte in wenigen Sekunden (< 1 Minute) passieren, weil die Daten nicht erneut kopiert werden. **Bitte dieses Vorgehen nach dem initialen Laden immer anwenden**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the table from the original location. The pattern of the path is always the same
# MAGIC -- Drop the table if it already exists to prevent error messages
# MAGIC DROP TABLE IF EXISTS food_facts;
# MAGIC CREATE TABLE food_facts USING DELTA LOCATION 'dbfs:/user/hive/warehouse/food_facts/';

# COMMAND ----------

# MAGIC %md
# MAGIC # Teil 1: Erkundung des Datensatzes 🔎
# MAGIC Aufgrund der Größe des Datensatzes müsst ihr den ersten Teil der Fallstudie innerhalb von Databricks bearbeiten. Für Teil 2 und 3 wird der Datensatz verkleinert und ihr könnt wahlweise in Databricks oder RStudio arbeiten.
# MAGIC 
# MAGIC 👇 Fügt eure Lösungen als Code-Blöcke direkt unter die jeweilige Frage ein. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Welche Dimensionierung hat der Datensatz?
# MAGIC ---
# MAGIC - Führt mehrere Analysen zum Umfang des Datensatzes durch. Schreibt die entsprechenden Statements in R oder SQL in einzelne Blöcke!
# MAGIC - Denkt auch an einen kurzen Kommentar zur Interpretation des Ergebnisses!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Welche Spalten hat der Datensatz?
# MAGIC ---
# MAGIC 
# MAGIC - Erstellt eine Liste aller Spalten!
# MAGIC - Extrahiert die unterschiedlichen Datentypen über alle Spalten und beschreibt kurz jeden Datentyp!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Welche Werte haben die Spalten?
# MAGIC ---
# MAGIC - Schaut euch exemplarisch die Spalten `additives`, `countries`, `lang` und `nutriscore_score` an!
# MAGIC - Prüft die vorhandenen Werte in diesen Spalten und ermittelt die Häufigkeiten, mit denen sie vorkommen! Bei welchen Spalten ist das problematisch und warum?
# MAGIC - Analysiert die genannten Spalten auf Vollständigkeit oder fehlende Werte!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Wie ist die Datenqualität der Spalten zu den Nährwertangaben?
# MAGIC ---
# MAGIC - Überprüft die Wertebereiche der Spalten!
# MAGIC - Wie ist die Häufigkeitsverteilung pro Spalte?
# MAGIC - Gibt es fehlende oder unplausible Werte?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Welche Bezeichnungen werden in der Spalte `countries` für Deutschland verwendet?
# MAGIC ---
# MAGIC - Die Spalte `countries` enthält die Information, in welchen Ländern ein Produkt zum Verkauf angeboten wird. Im Folgenden schauen wir insbesondere auf die Produkte, die in Deutschland angeboten werden.
# MAGIC - Überprüft, welche unterschiedlichen Bezeichnungen für Deutschland verwendet werden!
# MAGIC - Überlegt euch eine Lösung, um alle Produkte für Deutschland filtern zu können. Wendet die Lösung an und erstellt einen View oder Dataframe für die weitere Arbeit mit diesem Filter!
# MAGIC 
# MAGIC ⚠ **WICHTIGER HINWEIS**: Gebt euch bei dieser Aufgabe besonders Mühe! Das Ergebnis verwendet ihr für die Teile 2 und 3 als Basis.

# COMMAND ----------

# MAGIC %md
# MAGIC # Teil 2: Geschlossene Fragen 🎯
# MAGIC Im zweiten Teil erwarten euch mehr oder weniger geschlossene Fragestellungen, für die es zwar mehrere *technische* Lösungswege geben kann, bei denen aber ähnliche oder die selben Antworten herauskommen sollten. Ihr könnt euch hier insbesondere über einen effizienten Lösungsweg, gute Dokumentation sowie eine durchdachte Form der Darstellung differenzieren.
# MAGIC 
# MAGIC **⚠ WICHTIGER HINWEIS**: Für die folgenden Fragen sollt ihr nur Produkte einbeziehen, die in Deutschland verkauft werden (s. Ergebnis aus 1.5). Dadurch wird der Datensatz deutlich kleiner und ihr könnt die Analysen wahlweise in Databricks oder RStudio durchführen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Wie viele Produkte wurden pro Jahr neu hinzugefügt?
# MAGIC ---
# MAGIC - Die Spalte `created_t` enthält einen UNIX-Timestamp mit dem Zeitpunkt der Erstellung. Findet einen Weg, diesen in ein Datum umzuwandeln und das Jahr zu extrahieren.
# MAGIC - Das Jahr 2022 ist für diesen Datensatz unvollständig und daher nicht aussagekräftig. Lasst es daher bei der Analyse außen vor.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Wie ist die Verteilung des Energiegehalts pro 100 g für die verschiedenen Nutri-Score Label (A-E)?
# MAGIC ---
# MAGIC - Erstellt für diese Aufgabe eine geeignete **publizierfähige Visualisierung** mit `ggplot2`!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Wie ist die Verteilung des Nutri-Scores im Vergleich verschiedener Produktkategorien?
# MAGIC ---
# MAGIC - Betrachtet hier die detailliertere Angabe des Nutri-Scores als Zahl.
# MAGIC - Verwendet das Feld `categories_tags` für die Analyse. Vergleicht die Produktgruppen `en:beverages`, `en:dairies`, `en:meats` und `en:snacks`
# MAGIC - Erstellt für diese Aufgabe eine geeignete **publizierfähige Visualisierung** mit `ggplot2`!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Gibt es einen Zusammenhang zwischen dem Nutri-Score und dem Gehalt an gesättigten Fettsäuren?
# MAGIC ---
# MAGIC - <a href="https://www.bmel.de/DE/themen/ernaehrung/lebensmittel-kennzeichnung/freiwillige-angaben-und-label/nutri-score/nutri-score-erklaert-verbraucherinfo.html" target="_blank">Hier</a> findet ihr Infos zum Nutri-Score.
# MAGIC 
# MAGIC **Hinweise:** 
# MAGIC - Der Nutri-Score ist neben dem Buchstaben-Label auch als Zahl im Datensatz vorhanden.
# MAGIC - Ein Punktediagramm wird aufgrund der vielen Datenpunkte nicht gut funktionieren. Überlegt euch eine andere Form der Visualisierung.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Wie häufig werden Produkte mit dem Eco-Score gekennzeichnet? Wie unterscheiden sich Geschäfte und Marken dabei?
# MAGIC ---
# MAGIC - Versucht, die häufigsten Marken und Geschäfte zu harmonisieren und z. B. unterschiedliche Schreibweisen zu vereinheitlichen. Das erhöht die Qualität des Ergebnisses.
# MAGIC - Stellt absolute und relative Werte dar.
# MAGIC - Erstellt für diese Aufgabe mindestens eine geeignete **publizierfähige Visualisierung** mit `ggplot2`!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Welche fünf Marken verwenden am häufigsten Aspartam in ihren Produkten?
# MAGIC ---
# MAGIC - Versucht wie schon bei 2.7 unterschiedliche Schreibweisen bei Markennamen zu berücksichtigen und vereinheitlich sie.
# MAGIC - Welche Produktklassen sind dabei häufig vertreten?
# MAGIC - Erstellt für diese Aufgabe eine geeignete **publizierfähige Visualisierung** mit `ggplot2`!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Welche Zusatzstoffe kommen am häufigsten *zusammen* vor?
# MAGIC ---
# MAGIC - Für diese Aufgabe ist keine Visualisierung notwendig. Erstellt aber gerne eine, wenn ihr es für sinnvoll haltet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Was sind die fünf häufigsten Zutaten über alle Produkte?
# MAGIC ---
# MAGIC - Gebt sowohl den absoluten als auch den prozentualen Wert an.
# MAGIC - Für die Ermittlung der Zutaten kommen verschiedene Spalten in Frage. Vergleicht die Möglichkeiten und wählt die beste aus.
# MAGIC - Beschränkt euch auf Produkte, die in deutscher Sprache beschrieben wurden. 
# MAGIC - Versucht, die Schreibweisen der häufigsten Zutaten so weit wie möglich zu vereinheitlichen (falls notwendig).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9 Wie viele Produkte enthalten Zucker als Zutat?
# MAGIC ---
# MAGIC - Gebt sowohl die absolute als auch die relative Häufigkeit an!
# MAGIC - Prüft auch, wie viele Produkte Zucker als erste Zutat enthalten (und damit als Zutat mit dem größten Anteil)!
# MAGIC - Berücksichtigt in einer weiteren Analyse auch unterschiedliche Arten oder Bezeichnungen von Zucker! 
# MAGIC   - Führt eine eine Analyse durch, welche Art oder Bezeichnung wie häufig verwendet wird.
# MAGIC   - Erstellt für diese Aufgabe eine geeignete **publizierfähige Visualisierung** mit `ggplot2`!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.10 Haben Bio-Produkte im Durschnitt einen besseren Eco-Score?
# MAGIC ---
# MAGIC - Führt die Analyse im Sinne der explorativen Datenananalyse durch. Eine statistische Überprüfung der Hypothese ist nicht erforderlich!
# MAGIC - Erstellt für diese Aufgabe eine geeignete **publizierfähige Visualisierung** mit `ggplot2`!

# COMMAND ----------

# MAGIC %md
# MAGIC # Teil 3: Offene Fragen 👩‍🔬
# MAGIC ---
# MAGIC Im letzten Teil der Fallstudie seid ihr mit sehr offenen Fragen konfrontiert. Hier steht das gesamte Spektrum der explorativen Datenanalyse von der Ideengenerierung, über die Aufstellung von Hypothesen bis zur Entwicklung und Umsetzung von Lösungsstrategien im Fokus. Letztlich ist auch eure Kreativität hier stark gefordert. Für die folgenden Fragen gibt es kein richtig oder falsch. Es gibt nur gute oder weniger gute Ideen 💡, sinnvolle oder weniger sinnvolle Annahmen ✔, tiefgründige oder oberflächige Analysen 📉 sowie sorgfältige oder fehlerhafte Umsetzungen 👩‍💻.
# MAGIC 
# MAGIC **⚠ WICHTIGER HINWEIS**: Für die folgenden Fragen sollt ihr nur Produkte einbeziehen, die in Deutschland verkauft werden (s. Ergebnis aus 1.5). Dadurch wird der Datensatz deutlich kleiner und ihr könnt die Analysen wahlweise in Databricks oder RStudio durchführen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Vergleicht den Nutri-Score und den NOVA-Score miteinander!
# MAGIC ---
# MAGIC - Schaut euch das Paper <a href="https://pubmed.ncbi.nlm.nih.gov/34444941/" target="_blank">"Two Dimensions of Nutritional Value: Nutri-Score and NOVA"</a> dazu an und lasst euch von den Analysen der Autoren inspirieren.
# MAGIC - Verwendet für die Filterung der Daten ein Vorgehen angelehnt an die Abbildung 1 im genannten Paper. Adaptiert es entsprechend für den deutschen Markt.
# MAGIC - Sucht über die Analysen des Papers hinaus nach neuen Hypothesen und Fragen und beantwortet diese mit geeigneten Analysen.
# MAGIC - Erstellt für eure finalen Ergebnisse jeweils eine **publizierfähige Visualisierung** mit `ggplot2`! Mindestens jedoch drei!
# MAGIC - Das Paper findet ihr <a href="https://www.mdpi.com/2072-6643/13/8/2783/htm" target="_blank">hier als HTML-Version</a> und <a href="https://www.mdpi.com/2072-6643/13/8/2783/pdf?version=1629104070" target="_blank">hier als PDF</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Führt eine explorative Datenanalyse mit folgender Leitfrage durch: "Wie stehen die Zutaten der Lebensmittel im Zusammenhang mit dem Eco-Score?"
# MAGIC ---
# MAGIC - Diese Frage ist bewusst sehr offen gestellt und lässt viele Möglichkeiten der Herangehensweise zu.
# MAGIC - Führt eure Analyse nach dem bekannten Schema der explorativen Datenanalyse aus (Fragen, Hypothesen, Analysen, mehr Fragen, von Vorne ...)
# MAGIC - Erstellt für eure finalen Ergebnisse jeweils eine **publizierfähige Visualisierung** mit `ggplot2`! Mindestens jedoch drei!
