// Databricks notebook source
// MAGIC %md
// MAGIC # Laden der Daten (einmalig ausführen)
// MAGIC ---
// MAGIC Der folgende Block unten erstellt 4 neue Tabellen in eurem Databricks-Account:<br><br>
// MAGIC 
// MAGIC - `meta_Grocery_and_Gourmet_Food`
// MAGIC - `meta_Pet_Supplies`
// MAGIC - `reviews_Grocery_and_Gourmet_Food`
// MAGIC - `reviews_Pet_Supplies`
// MAGIC 
// MAGIC Ihr müsst diesen Block nur einmal ausführen, die Tabellen bleiben permanent in eurem Account gespeichert. Auch nachdem ihr euch ausloggt oder ein neues Cluster erstellt. Ihr könnt den Block mit dem kleinen Play-Symbol oben rechts ausführen. Alternativ könnt ihr Strg + Enter drücken, wenn ihr euch mit dem Cursor innerhalb des Blocks befindet.
// MAGIC 
// MAGIC ---
// MAGIC 
// MAGIC **HINWEIS:** Die Ausführung kann einige Minuten dauern. Es werden immerhin komprimierte ~550 MB über die Leitung zwischen Amazon S3 und Databricks übertragen.

// COMMAND ----------

import scala.sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType}

var tables = Array("meta_Pet_Supplies")
tables = tables :+ "meta_Grocery_and_Gourmet_Food"
tables = tables :+ "reviews_Pet_Supplies"
tables = tables :+ "reviews_Grocery_and_Gourmet_Food"

val file_ending = ".json.gz"

for(t <- tables) {

  val tableName = t
  dbutils.fs.rm("file:/tmp/" + tableName + file_ending)

  "wget -P /tmp https://s3.amazonaws.com/nicolas.meseth/amazon_reviews/" + tableName + file_ending !!

  val localpath = "file:/tmp/" + tableName + file_ending
  dbutils.fs.rm("dbfs:/datasets/" + tableName + file_ending)
  dbutils.fs.mkdirs("dbfs:/datasets/")
  dbutils.fs.cp(localpath, "dbfs:/datasets/")
  display(dbutils.fs.ls("dbfs:/datasets/" +  tableName + file_ending))

  sqlContext.sql("drop table if exists " + tableName)
  
  var df = spark.read
                        .option("inferSchema", "true")
                        .option("escape", "\"")
                        .option("allowBackslashEscapingAnyCharacter", true)
                        .json("/datasets/" +  tableName + file_ending)
 
 // Conversions
 if(tableName == "reviews_Pet_Supplies" || tableName == "reviews_Grocery_and_Gourmet_Food") {
    df = df.withColumn("unixReviewTime", $"unixReviewTime".cast("timestamp"))
    df = df.withColumn("reviewTime", unix_timestamp($"reviewTime","MM dd, yyyy").cast("timestamp"))
  }
      
  df.unpersist()
  df.cache()
  df.write.saveAsTable(tableName);  
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Überprüfen der Tabellen
// MAGIC ---
// MAGIC Die folgenden SQL-Abfragen geben euch die Rückmeldung, ob alles geklappt hat. Für jede Tabelle wird die Anzahl Datensätze in der Spalte `Num Records` ausgegeben.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 110.707 Einträge
// MAGIC select count(1) as `Num Records`, 'meta_Pet_Supplies' as `Table` from meta_Pet_Supplies
// MAGIC union
// MAGIC -- 1.235.316 Einträge
// MAGIC select count(1) as `Num Records`, 'reviews_Pet_Supplies' as `Table` from reviews_Pet_Supplies
// MAGIC union
// MAGIC -- 171.760 Einträge
// MAGIC select count(1) as `Num Records`, 'meta_Grocery_and_Gourmet_Food' as `Table` from meta_Grocery_and_Gourmet_Food
// MAGIC union
// MAGIC -- 1.297.156 Einträge
// MAGIC select count(1) as `Num Records`, 'reviews_Grocery_and_Gourmet_Food' as `Table` from reviews_Grocery_and_Gourmet_Food
// MAGIC order by `Table`

// COMMAND ----------

// MAGIC %md
// MAGIC # Jetzt seid ihr an der Reihe ...
// MAGIC ---
// MAGIC Ihr könnt nun direkt unter diesem Block loslegen und die Tabellen und darin enthaltenen Daten abfragen. Viel Erfolg!

// COMMAND ----------

// MAGIC %sql
// MAGIC describe meta_Pet_Supplies

// COMMAND ----------

// MAGIC %md
// MAGIC # Zugriff auf bestimmte Elemente eines Arrays
// MAGIC ---
// MAGIC Mit dem Index in eckigen Klammern könnt ihr auch eine beliebiges Element eines Array zugreifen. Beachtet, dass das erste Elemente die Position 0 hat.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Zugriff auf die zweite Kategorie des Produktes (wenn vorhanden)
// MAGIC -- Produkte können mehr als eine Kategorie haben
// MAGIC select title, categories[1] 
// MAGIC from meta_Pet_Supplies
// MAGIC -- nur Datensätze bei denen eine zweite Kategorie existiert
// MAGIC where categories[1] is not null

// COMMAND ----------

// MAGIC %md
// MAGIC # Zugriff auf mehrer Elemente eines Arrays in einer Abfrage
// MAGIC ---
// MAGIC Für die Kategorien bietet es sich an, die 5 Ebenen jeweils als eigene Spalte darzustellen.

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace view meta_Pet_Supplies_Categories as
// MAGIC select title
// MAGIC       ,categories[0][0] as `Level 1`
// MAGIC       ,categories[0][1] as `Level 2` 
// MAGIC       ,categories[0][2] as `Level 3` 
// MAGIC       ,categories[0][3] as `Level 4` 
// MAGIC       ,categories[0][4] as `Level 5` 
// MAGIC from meta_Pet_Supplies
// MAGIC -- Nur für die bei denen Level 2 = Dogs
// MAGIC where categories[0][1] = 'Dogs'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from meta_pet_supplies_categories

// COMMAND ----------

// MAGIC %md
// MAGIC # Die Größe eines Array ermitteln
// MAGIC ---
// MAGIC Häufig ist es notwendig, die Anzahl Elemente eines Array zu kennen. In den meisten Fällen sind wir daran interessiert, wie viele Elemente in der längsten Liste vorhanden sind.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Wie viele Produktkategorien hat ein Produkt maximal
// MAGIC select size(categories)
// MAGIC from meta_Pet_Supplies
// MAGIC order by size(categories) desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select asin
// MAGIC       ,title
// MAGIC       ,categories[0][0] as `Level 1`
// MAGIC       ,categories[0][1] as `Level 2` 
// MAGIC       ,categories[0][2] as `Level 3` 
// MAGIC       ,categories[0][3] as `Level 4` 
// MAGIC       ,categories[0][4] as `Level 5` 
// MAGIC from meta_Pet_Supplies
// MAGIC where categories[0][0] is not null
// MAGIC 
// MAGIC UNION
// MAGIC 
// MAGIC -- Eine mögliche 2. Kategorie, falls vorhanden
// MAGIC select asin
// MAGIC       ,title
// MAGIC       ,categories[1][0] as `Level 1`
// MAGIC       ,categories[1][1] as `Level 2` 
// MAGIC       ,categories[1][2] as `Level 3` 
// MAGIC       ,categories[1][3] as `Level 4` 
// MAGIC       ,categories[1][4] as `Level 5` 
// MAGIC from meta_Pet_Supplies
// MAGIC where categories[1][0] is not null
// MAGIC 
// MAGIC UNION
// MAGIC 
// MAGIC -- Eine mögliche 3. Kategorie, falls vorhanden
// MAGIC select asin 
// MAGIC       ,title
// MAGIC       ,categories[2][0] as `Level 1`
// MAGIC       ,categories[2][1] as `Level 2` 
// MAGIC       ,categories[2][2] as `Level 3` 
// MAGIC       ,categories[2][3] as `Level 4` 
// MAGIC       ,categories[2][4] as `Level 5` 
// MAGIC from meta_Pet_Supplies
// MAGIC where categories[2][0] is not null
// MAGIC 
// MAGIC UNION
// MAGIC 
// MAGIC -- Eine mögliche 4. Kategorie, falls vorhanden
// MAGIC select asin
// MAGIC       ,title
// MAGIC       ,categories[3][0] as `Level 1`
// MAGIC       ,categories[3][1] as `Level 2` 
// MAGIC       ,categories[3][2] as `Level 3` 
// MAGIC       ,categories[3][3] as `Level 4` 
// MAGIC       ,categories[3][4] as `Level 5` 
// MAGIC from meta_Pet_Supplies
// MAGIC where categories[3][0] is not null
// MAGIC 
// MAGIC order by asin desc

// COMMAND ----------

// MAGIC %md
// MAGIC # Prüfen, ob ein Element in einem Array vorhanden ist
// MAGIC --
// MAGIC Mit `array_contains` können wir in einem Array direkt nach einem bestimmten Element suchen.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Alle Produkte mit Level 1 = Dogs und NICHT Level 1 = Health Supplies
// MAGIC select title, categories[0]
// MAGIC from meta_Pet_Supplies
// MAGIC where array_contains(categories[0], 'Dogs')
// MAGIC and not array_contains(categories[0], 'Health Supplies')

// COMMAND ----------

// MAGIC %md
// MAGIC # Explode zerlegt ein Array in Zeilen

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Explode Funktion zerlegt einen Array in eizelne Zeilen
// MAGIC select asin, title, col as `Subcategory`, pos
// MAGIC from (
// MAGIC   -- Zweite Liste in Zeilen zerlegen (Level jeder Kategorie)
// MAGIC   select asin, title, posexplode(categories_exploded)
// MAGIC   from (
// MAGIC     -- Erste Liste in Zeilen zerlegen (Kategorien des Produktes)
// MAGIC     select asin, title, explode(categories) as `categories_exploded`
// MAGIC     from meta_Pet_Supplies
// MAGIC   )
// MAGIC )
// MAGIC -- Filtern auf Level 1 (Top-Kategorie)
// MAGIC where pos in (0,1,2)
