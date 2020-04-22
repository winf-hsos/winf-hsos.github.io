-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Date and Time with SQL
-- MAGIC ---
-- MAGIC This notebook contains examples on how to work with timestamp columns in SQL.
-- MAGIC 
-- MAGIC **Functions we'll learn in this notebook:**
-- MAGIC 
-- MAGIC - `day()`, `dayofmonth()`, `dayofweek()`, `dayofyear()`, `weekday()` - to get specific information regarding the day from a date
-- MAGIC - `month()` - to extract the month from a date 
-- MAGIC - `weekofyear()` - to extract the week of the year (0-52) from a date
-- MAGIC - `year()` - to extract the year from a date
-- MAGIC - `date_format()` - to format a timestamp
-- MAGIC - `datediff()` - to calculate the difference in days between two dates
-- MAGIC - `datetrunc()` - to truncate a given date to a certain date part
-- MAGIC - `date_sub()` - to substract a given number of days from a date
-- MAGIC - `date_add()` - to add a given number of days to a date
-- MAGIC - `months_between()` - to calculate the months between two dates
-- MAGIC - `now()` - to get the current timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Extract date parts from a timestamp column

-- COMMAND ----------

select created_at as `Timestamp`
      ,day(created_at) as `Day`
      ,dayofmonth(created_at) as `Day of month`
      ,dayofweek(created_at) as `Day of week`
      ,dayofyear(created_at) as `Day of year`
      ,weekday(created_at) as `Weekday (0 = Monday)` 
      ,weekofyear(created_at) as `Week of Year`
      ,month(created_at) as `Month`
      ,year(created_at) as `Year`
      ,date_format(created_at, 'YYYY-MM') as `Display Month`      
from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Extract time parts from a timestamp column

-- COMMAND ----------

select created_at as `Timestamp`
      ,hour(created_at) as `Hour`
      ,minute(created_at) as `Minute`
      ,second(created_at) as `Second`
      ,date_format(created_at, 'mm') as `Minute Double Digits`      
      ,date_format(created_at, 'HH') as `Hour Double Digits`      
      ,date_format(created_at, 'HH:mm') as `Time Formatted 24h`      
from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Calculate with dates

-- COMMAND ----------

select created_at
      ,insert_timestamp
      ,datediff(now(), created_at) as `Time since the tweet in days`
      ,date_sub(created_at, 10) as `Ten days before the tweet`
      ,date_add(created_at, 10) as `Ten days after the tweet`
      ,months_between(now(), created_at) `Months since the tweet`
from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Examples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How is the number of tweets distributed over the month in the current year?

-- COMMAND ----------

select date_format(created_at, 'YYYY-MM') as `Month`
      ,count(*) as `Number Tweets`
from tweets
where year(created_at) = year(now())
group by `Month`
order by `Month`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How many tweets are there per day in the current year, distinguised by weekend or workday?

-- COMMAND ----------

select date_format(created_at, 'MM-dd') as `Day`
      ,case when dayofweek(created_at) = 6  or dayofweek(created_at) = 7 then 'Yes'
            else 'No' end
            as `Is weekend`
      ,count(*) as `Number Tweets`
from tweets
where year(created_at) = year(now())
group by `Day`, `Is Weekend`
order by `Day`, `Is Weekend`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What are the favorite tweet times of the day?

-- COMMAND ----------

select hour(created_at) as `Hour`
      ,count(*) as `Number Tweets`
from tweets
group by hour(created_at)
order by hour(created_at) asc
