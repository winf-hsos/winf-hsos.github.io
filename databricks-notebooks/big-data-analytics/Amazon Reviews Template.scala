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
