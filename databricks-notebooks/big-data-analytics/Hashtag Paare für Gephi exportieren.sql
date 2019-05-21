-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Aus Hashtags eine Themenlandkarte erstellen
-- MAGIC ---
-- MAGIC Eine Möglichkeit, Themen in Twitter-Daten zu identifizieren, besteht darin, die Verwendung von Hashtags systematisch zu analysieren. Um speziell zusammenhängende Themen zu finden, bietet es sich an, die Hashtags zu zählen, die häufig paarweise auftreten. Visualisiert man anschließend die gefundenen Hashtag-Paare und deren Häufigkeiten in geeigneter Weise, so bekommt man eine *Themenlandkarte* der Twitter-Hashtags.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View für Hashtags erstellen

-- COMMAND ----------

create or replace view hashtags as
  -- Alle Hashtags in Kleinschreibung, um einfachere Vergleiche zu ermöglichen
  select id
        ,lower(hashtag) as hashtag
        ,created_at
  from (
    select id, explode(hashtags) as hashtag, created_at
    from twitter_timelines
  )

-- COMMAND ----------

select * from hashtags

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View für das Zählen der Hashtag-Paare

-- COMMAND ----------

create or replace view hashtag_pairs as
select distinct
   id
   ,case when h1 > h2 then h2 else h1 end as h1
   ,case when h1 < h2 then h2 else h1 end as h2 
   from (
     select h1.id
           ,h1.hashtag as h1
           ,h2.hashtag as h2 
     from 
        (select id, hashtag from hashtags where year(created_at) = '2018') h1
     inner join 
        (select id, hashtag from hashtags where year(created_at) = '2018') h2
     on h1.id = h2.id
     and h1.hashtag <> h2.hashtag
  )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from hashtag_pairs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL-Abfrage für den Export der Hashtag-Paare (Gephi)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Export der Knoten (Hashtags)

-- COMMAND ----------

create or replace view hashtag_export as
select hashtag as `Id`
      ,hashtag as `Label`
      ,count(1) as `Size`
from hashtags
where year(created_at) = '2018'
group by hashtag
-- Nur Hashtags, die häufiger als 20 Mal vorkamen
having count(1) > 10

-- COMMAND ----------

select * from hashtag_export
order by Size desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Export der Kanten (Hashtag-Paare)

-- COMMAND ----------

select h1 as `Source`
      ,h2 as `Target`
      ,count(1) as `Weight`
from hashtag_pairs 
where h1 in (select Id from hashtag_export)
and h2 in (select Id from hashtag_export)
group by h1, h2
order by count(1) desc
