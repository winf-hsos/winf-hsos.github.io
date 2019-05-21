-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Image Analytics mit Azure ML
-- MAGIC ---
-- MAGIC **Achtung:** Dieses Notebook geht davon aus, dass zuvor die Tabelle `twitter_timelines` und `twitter_followers` geladen wurde.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anlegen eines View für den Profilfoto-Export
-- MAGIC ---
-- MAGIC Im ersten Schritt kümmern wir uns um die Profilbilder. Hier stehen wir vor der Herausforderung, dass die Tabelle `twitter_followers` unsere geladenen Accounts möglicherweise mehrfach enthält. Das ist dann der Fall, wenn ein von uns geladener Account gleichzeitig mehreren unserer anderen geladenen Accounts folgt. Für jeden Account, dem gefolgt wird, bekommen wir einen Eintrag. Da diese Einträge zu unterschiedlichen Zeitpunkten abgerufen wurden, und wir die Information darüber haben, wann genau das für jeden Datensatz der Fall war, können wir mit dem folgenden View jeweils die neueste Version des Accounts exportieren.

-- COMMAND ----------

create or replace view twitter_profile_images as
select screen_name, profile_image from 
(
  select screen_name
        -- Entfernen des _normal Zusatzes, um größeres Bild zu bekommen
        ,regexp_replace(profile_image_url_https, '_normal', '') as profile_image
        ,rank() over (partition by screen_name order by retrieved_time desc) as `rank`
  from twitter_followers
  where screen_name in (select distinct follower_of from twitter_followers)
) 
-- Jeweils nur den aktuellsten Datensatz pro Account
where rank = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Daten aus dem Profilfotos-View exportieren
-- MAGIC ---
-- MAGIC Mit der folgenden Abfrage können nun die Profilfotos der von euch identifizierten Accounts exportiert werden.

-- COMMAND ----------

select screen_name as `id`
      ,profile_image as `imageUrl`
from twitter_profile_images
where 1 = 1
-- Hier folgen eure eigenen Filter, um die URLs zu reduzieren
order by id
limit 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exportieren der Fotos aus Tweets
-- MAGIC ---
-- MAGIC Analog zu den Profilbildern erlaubt das folgende SQL uns die Tweets mit Fotos zu identifzieren und daraus die Foto-URLs zu exportieren:

-- COMMAND ----------

select id
      ,photo.media_url_https as `imageUrl`
from (
  select id, explode(photos) as `photo`
  from twitter_timelines
  where array_contains(hashtags, 'organic')
)
limit 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Hochladen der CSV-Datei im Big Data Analytics Helper Tool
-- MAGIC ---
-- MAGIC Das Tool zum Hochladen der Texte findet ihr hier: <a href="https://big-data-analytics-helper.glitch.me/text/index.html" target="_blank">Link zum Tool</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Laden der Ergebniss als Tabellen
-- MAGIC ---
-- MAGIC Nachdem ihr die CSV-Datei in das Big Data Analytics Helper Tool hochgeladen habt, werden die Ergebnisse des Azure Modells als JSON-File zurückgeliefert, das ihr mit dem folgenden Codeblock direkt als Tabelle in Databricks laden könnt. Ihr verfügt im Anschluss über die neue Tabelle `vision`:<br>
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
-- MAGIC val tables = Array("vision")
-- MAGIC 
-- MAGIC val file_ending = ".json"
-- MAGIC val group_code = dbutils.widgets.get("group_code")
-- MAGIC 
-- MAGIC for(t <- tables) {
-- MAGIC 
-- MAGIC   val tableName = t
-- MAGIC   var localFileName = "azure%2Fvision%2F" + group_code + "%2Foutput" + file_ending + "?alt=media"
-- MAGIC   val localpath = "file:/tmp/" + localFileName
-- MAGIC   dbutils.fs.rm("file:/tmp/" + localFileName)
-- MAGIC 
-- MAGIC   var url = "https://firebasestorage.googleapis.com/v0/b/big-data-analytics-helper.appspot.com/o/azure%2Fvision%2F" + group_code + "%2Foutput" + file_ending + "?alt=media"  
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
-- MAGIC ## Ergebnisse der Bildanalyse filtern und sichten
-- MAGIC ---
-- MAGIC Um die Ergebnisse der Bildanalyse zu sichten ist es sinnvoll, fehlerhafte Analysen zu filtern. Diese erkennen wir daran, dass in der Spalte `code` ein Wert (und in der Spalte `message` eine Fehlermeldung) vorhanden ist. Das folgende Statement filtert diese Datensätze heraus und selektiert alle Ergebnisspalten.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Unterscheiden zwischen Profilfotos und Fotos aus Tweets in den Ergebnissen
-- MAGIC ---
-- MAGIC Die Ergebnisse der analysierten Fotos landen allesamt in der gleichen Tabelle `vision`, ganz gleich ob es sich um Profilfotos oder Fotos aus Tweets handelte. Um ex post eine Unterscheidung vorzunehmen, kann geprüft werden, ob die Spalte `id` in eine ganze Zahl umgewandelt werden kann (Foto aus Tweet), oder nicht (Profilbild).

-- COMMAND ----------

create or replace view vision_result as
select *
    ,case when cast(id as bigint) is null then 'profile' 
         else 'tweet' end as `image_source`
from vision
where code is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Beispielanalyse 1: Wie alt sind die Personen auf den Profilfotos?

-- COMMAND ----------

select id, url, face.age, face.gender
from (
  select id, url, explode(faces) as `face` 
  from vision_result
  where image_source = 'profile'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Beispielanalyse 2: Welche Überschriften tragen die Fotos in den Tweets?

-- COMMAND ----------

select id
      ,url
      ,caption.text 
from (
select id, explode(description.captions) as `caption`, url from vision_result
where image_source = 'tweet'
)
where caption.confidence > 0.2


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Beispielanalyse 3: Was ist in den Bildern in den Tweets zu sehen?

-- COMMAND ----------

select id
      ,url
      ,tag.name as `tag`
from (
  select id, url, explode(tags) as `tag`
  from vision_result
  where image_source = 'tweet'
)
where tag.confidence > 0.75

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exkurs: Ein Bild in Databricks anzeigen
-- MAGIC ---
-- MAGIC Databricks ist in der Lage, Bilder anzuzeigen:

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC var url = "https://pbs.twimg.com/profile_images"
-- MAGIC var id = "260501364"
-- MAGIC var image = "philip.jpg"
-- MAGIC 
-- MAGIC "wget -P /tmp " + url + "/" + id + "/" + image !!
-- MAGIC val df = spark.read.format("image").load("file:///tmp/" + image)
-- MAGIC display(df)
