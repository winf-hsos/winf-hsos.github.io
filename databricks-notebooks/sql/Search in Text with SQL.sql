-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Search in text columns with SQL
-- MAGIC ---
-- MAGIC In this notebook, you'll find examples how to search in text columns using SQL.
-- MAGIC 
-- MAGIC **Functions we'll learn in this notebook:**<br><br>
-- MAGIC 
-- MAGIC - The `like`-operator
-- MAGIC - The `rlike`-operator
-- MAGIC - A user-defined-function (UDF) called `str_contains()` to search in strings more flexibly
-- MAGIC - The functions `collect_list()` and `collect_set()`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The `like`-operator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can use the `like`-operator to search for simple wildcard patterns in text columns. The query below returns all tweets containing the string 'covid' anywhere in it.
-- MAGIC 
-- MAGIC **NOTE:** Spark SQL is case-sensitive. 'covid' is a different string than 'Covid'.

-- COMMAND ----------

select screen_name
      ,text
from tweets
where text like '%covid%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To avoid differences that arise from users writing 'Covid' and those writing 'covid', we can apply the function `lower()` to the text and compare that to 'covid':

-- COMMAND ----------

select screen_name
      ,text
from tweets
where lower(text) like '%covid%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can play with the wildcard symbol `%` to change the pattern we are looking for:

-- COMMAND ----------

-- Tweet must start with 'covid'
select screen_name
      ,text
from tweets
where lower(text) like 'covid%'

-- COMMAND ----------

-- Tweet must end in the string 'covid'

-- COMMAND ----------

select screen_name
      ,text
from tweets
where text like '%covid'

-- COMMAND ----------

-- Tweet must contain 'covid' and 'virus', in that order
select screen_name
      ,text
from tweets
where text like '%covid%virus%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The `rlike`-operator
-- MAGIC With `rlike`, we can use the flexibility of regular expressions:

-- COMMAND ----------

select
  lower(text)
from
  tweets
where rlike(lower(text), '(corona|covid)')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A user defined function `str_contains()`
-- MAGIC ---
-- MAGIC In Databricks we can switch between the languages SQL, Python, Scala, and R (plus Markdown and Bash-Scripts). This allows us to define and register our own SQL functions with Scala:

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def strContains(s: String, k: String): Boolean = {
-- MAGIC   val str = Option(s).getOrElse(return false)
-- MAGIC   val keyword = Option(k).getOrElse(return false)
-- MAGIC   return keyword.r.findAllIn(str).length > 0
-- MAGIC }
-- MAGIC 
-- MAGIC val strContainsUDF = udf[Boolean, String, String](strContains)
-- MAGIC spark.udf.register("str_contains", strContainsUDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once we have registered a user-defined-function (UDF), we can use it in our SQL statements.

-- COMMAND ----------

select screen_name
      ,text
from tweets 
where str_contains(text, 'covid')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What did we win in comparison to the `like`-operator? We can use the function in flexible ways, for example in a join condition.
-- MAGIC 
-- MAGIC In the first step, we create a new table to store our keywords in, so we don't have to repeat them in every SQL statement where we need them:

-- COMMAND ----------

drop table if exists keywords;
create table keywords ( keyword string );
insert into keywords values ('covid');
insert into keywords values ('corona');
insert into keywords values ('merkel');

-- Check the result
select * from keywords;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next, we can take the new table `keywords` and join it with our `tweets` table using the `str_contains()` function as the join condition:

-- COMMAND ----------

select t.text
      ,k.keyword
from tweets t
left join keywords k
      on str_contains(t.text, k.keyword)
-- Keep only hits
where k.keyword is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `collect_list()` and `collect_set()`
-- MAGIC ---
-- MAGIC The result from the previous block contains one rows for each keyword in a tweet. This means we can have one tweet more than once in the result set, if the tweet contains more than one of the keywords. 
-- MAGIC 
-- MAGIC We can change that and apply `collect_list` to the keyword column. This will create an array of keyword hits for each tweet:

-- COMMAND ----------

select t.text
      ,collect_list(k.keyword) as `keywords`
from tweets t
left join keywords k
      on str_contains(t.text, k.keyword)
-- Keep only hits
where k.keyword is not null
group by t.text
order by size(keywords) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the query above, we applied `collect_list()` to the column `keyword`. Because this is effectively an aggregation function, we need to group the result by the other column `text` (line 8).
-- MAGIC 
-- MAGIC To see what happens, we sort the result by the length of the new array `keywords`, which tells us how many hits we got in a tweet. Because we sort in descending order, the tweets with the most hits show on top.
-- MAGIC 
-- MAGIC You'll also notice that if a tweet contains the same keyword twice, that keyword appears twice in the `keywords` array, too. If we want only unique hits, we can instead use `collect_set()`:

-- COMMAND ----------

select t.text
      ,collect_set(k.keyword) as `keywords`
from tweets t
left join keywords k
      on str_contains(t.text, k.keyword)
-- Keep only hits
where k.keyword is not null
group by t.text
order by size(keywords) desc
