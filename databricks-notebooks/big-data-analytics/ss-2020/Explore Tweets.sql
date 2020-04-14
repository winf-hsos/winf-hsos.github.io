-- Databricks notebook source
-- MAGIC %md
-- MAGIC # What columns does a tweet have?

-- COMMAND ----------

describe tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How many tweets are there?

-- COMMAND ----------

select count(*) from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How many tweets are there by user?

-- COMMAND ----------

select count(*) as `Number Tweets`
      ,screen_name as `User`
from tweets
group by screen_name
order by count(*) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How many tweets are there over time?

-- COMMAND ----------

select month(created_at) as `Month`
      ,year(created_at) as `Year`
      ,year(created_at) || '-' || month(created_at) as `Period`
      ,count(1)
from tweets
where year(created_at) IN (2019, 2020)
group by month(created_at), year(created_at)
order by year(created_at), month(created_at)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How many tweets are there per hour of the day?

-- COMMAND ----------

select hour(created_at) as `Hour`
      ,count(*) as `Number Tweets`
from tweets
group by hour(created_at)
order by hour(created_at) asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # What is the top 10 of the most popular tweets?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using favorite count

-- COMMAND ----------

select screen_name
      ,text
      , favorite_count
from tweets
order by favorite_count desc
limit 10
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using retweet count

-- COMMAND ----------

select screen_name
      ,text
      ,retweet_count
from tweets
where is_retweet = false
order by retweet_count desc
limit 10 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How many hashtags does a tweet have on average?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Count elements in an array

-- COMMAND ----------

select size(hashtags) as `Number Hashtags`
      ,hashtags
from tweets
order by size(hashtags) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculate the average across all tweets

-- COMMAND ----------

select avg(size(hashtags))
from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How many tweets contain a specific hashtag?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filter tweets by hashtag

-- COMMAND ----------

select text
      ,hashtags
from tweets
where array_contains(hashtags, 'covid19')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Counting the tweets with a specific hashtag

-- COMMAND ----------

select count(*) as `Number Tweets`
from tweets
where array_contains(hashtags, 'covid19')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # How long is the average tweet?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting the length of a text

-- COMMAND ----------

select length(text)
      ,text
from tweets
order by length(text) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating the average length

-- COMMAND ----------

select avg(length(text))
from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Who is most often retweeted by users in our data set?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## All twitter users

-- COMMAND ----------

select retweeted_user
      ,count(*) as `Number Retweeted`
from tweets
where is_retweet = true
group by retweeted_user
order by count(*) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Only users in our data set

-- COMMAND ----------

select retweeted_user
      ,count(*) as `Number Retweeted`
from tweets
where is_retweet = true
and retweeted_user in (select distinct screen_name from tweets)
group by retweeted_user
order by count(*) desc
