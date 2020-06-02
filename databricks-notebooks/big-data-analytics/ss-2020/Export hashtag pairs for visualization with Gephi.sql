-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Export hashtag pairs for visualization with Gephi
-- MAGIC ---
-- MAGIC One quick way to identify topics in tweets is to analyze the use of keywords or hashtags systematically. A common approach is to find pairs of hashtags (or words) that are often mentioned together in the same tweets. Visualizing the result such that pairs that often appear together are drawn close to each other gives us a visual way to explore a topic map.
-- MAGIC 
-- MAGIC In this notebook, you'll learn how to export hashtags as nodes and edges for visualization with <a href="https://gephi.org/users/download/" target="_blank">Gephi</a>.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Create view for hashtags and pairs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a view to explode hashtags

-- COMMAND ----------

create or replace view hashtags as
   -- Make all hashtags lower case for easier comparison
  select id
        ,lower(hashtag) as hashtag
        ,created_at
  from (
    select id, explode(hashtags) as hashtag, created_at
    from tweets
  )

-- COMMAND ----------

select * from hashtags

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create the view for counting hashtag pairs

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
        (select id, hashtag from hashtags where year(created_at) = '2020') h1
     inner join 
        (select id, hashtag from hashtags where year(created_at) = '2020') h2
     on h1.id = h2.id
     and h1.hashtag <> h2.hashtag
  )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from hashtag_pairs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Export nodes and edges as CSV for Gephi

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Export nodes

-- COMMAND ----------

create or replace view hashtag_export as
select hashtag as `Id`
      ,hashtag as `Label`
      ,count(1) as `Size`
from hashtags
where year(created_at) = '2020'
group by hashtag
-- Only hashtags that occured at least a number of times
having count(1) > 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can now select all columns from the view and download the result as CSV:

-- COMMAND ----------

select * from hashtag_export
order by `Size` desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Export edges (or links)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The following query extracts the corresponding hashtag paris, which are the links (or edges). The query renames the columns as they are expected in Gephi. 
-- MAGIC 
-- MAGIC The column weight reflects the number of times two hashtags were mentioned. For large data, it can be useful to include only pairs that were mentioned a minimum number of times:

-- COMMAND ----------

select h1 as `Source`
      ,h2 as `Target`
      ,count(1) as `Weight`
from hashtag_pairs 
where h1 in (select Id from hashtag_export)
and h2 in (select Id from hashtag_export)
group by h1, h2

-- Limit to pairs that occured at least n times
having count(1) > 2

order by `Weight` desc
