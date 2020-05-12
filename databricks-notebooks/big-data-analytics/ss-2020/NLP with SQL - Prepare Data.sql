-- Databricks notebook source
-- MAGIC %md
-- MAGIC # NLP with SQL - Prepare Data
-- MAGIC ---
-- MAGIC In this workbook, we'll go through five steps to prepare text data for analysis with SQL. The five steps we'll look at are:<br><br>
-- MAGIC 
-- MAGIC 1. **Filter raw data**. The results of this step will be stored on the view `tweets_filtered`.
-- MAGIC 2. **Clean and normalize**. The results of this step will be stored on the view `tweets_cleaned`.
-- MAGIC 3. **Tokenize**. The results of this step will be stored on the view `tweets_tokenized`.
-- MAGIC 4. **Filter stop words**. The results of this step will be stored on the view `tweets_stop`.
-- MAGIC 4. **Stemming and POS tagging**. The results of this step will be stored on the view `tweets_pos`.
-- MAGIC 
-- MAGIC 
-- MAGIC **Functions we'll learn/use in this notebook:**<br><br>
-- MAGIC 
-- MAGIC - `regexp_replace()` to replace string patterns in text columns
-- MAGIC - `split()` to transform a text into tokens using a separator symbol
-- MAGIC - `explode()` to separate a list of words into rows

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Filter raw data
-- MAGIC ---
-- MAGIC Before we apply compute-intensive operations such as tokenizing in the following, it is a good idea to first limit the data (rows) to what we actually need. In the example below, we'll simply filter the tweets on the hashtag *covid19*. You will likely have a more sophisticated filter for your real-world question.
-- MAGIC 
-- MAGIC Once we are satisifed with our filter, we create a view to access the filtered data easily in the subsequent code blocks. In this view, we only select the columns we are going to need in the following analysis.

-- COMMAND ----------

select id
      ,screen_name
      ,created_at
      ,text
      ,hashtags
from tweets
where array_contains(hashtags, 'covid19')

-- COMMAND ----------

-- Create the view
create or replace view tweets_filtered as
select id
      ,screen_name
      ,created_at
      ,text
      ,lang
      ,hashtags
from tweets
--where array_contains(hashtags, 'covid19')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can now access our filtered data using the new view.

-- COMMAND ----------

select * from tweets_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Clean and normalize text
-- MAGIC ---
-- MAGIC In the next step, we are going to clean-up our text column a bit. That is, we remove things that are irrelevant to our further analysis. We also convert everything to lower case. The statement below performs the following steps in that order:<br><br>
-- MAGIC 
-- MAGIC 1. Remove two or more subsequent white spaces
-- MAGIC 2. Remove special characters
-- MAGIC 3. Convert to lower case
-- MAGIC 4. Replace line breaks
-- MAGIC 
-- MAGIC Most of the work is done using the function `regexp_replace()`, which allows us to replace a certain string pattern in a text column using a <a href="https://en.wikipedia.org/wiki/Regular_expression" target="_blank">regular expression</a>.

-- COMMAND ----------

select id 
      ,screen_name
      ,text as original_text
      ,lang
      ,created_at
       -- Remove white spaces at the beginning or end
      ,trim(
         -- Remove two or more subsequent white spaces
         regexp_replace(
           -- Remove special characters
           regexp_replace(
             -- Convert to lower case
               lower(
                 -- Replace line breaks (2 different types)
                 regexp_replace(
                 regexp_replace(text, '\n', ' '), '\r', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ')) as `text`
from tweets_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Again, we'll store the result of this step on a view:

-- COMMAND ----------

create or replace view tweets_cleaned as
select id 
      ,screen_name
      ,text as original_text
      ,lang
      ,created_at
       -- Remove white spaces at the beginning or end
      ,trim(
         -- Remove two or more subsequent white spaces
         regexp_replace(
           -- Remove special characters
           regexp_replace(
             -- Convert to lower case
               lower(
                 -- Replace line breaks (2 different types)
                 regexp_replace(
                 regexp_replace(text, '\n', ' '), '\r', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ')) as `text`
from tweets_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With the view, we have easy acces to the data from step 2:

-- COMMAND ----------

select * from tweets_cleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Additional cleaning for tweets
-- MAGIC Since we are dealing with tweets here, we might want to remove some particular fragments, as those are not relevant for our further analysis AND are additionally contained in separate fields:<br><br>
-- MAGIC 
-- MAGIC - Hashtags that start with the #-symbol
-- MAGIC - User mentions that start with the @-symbol
-- MAGIC - URLs
-- MAGIC 
-- MAGIC **HINT:** You need to apply the following replacements before you clean the tweets with the statement from above. Otherwise the special characters `#` and `@` are gone, as well as some of the symbols that make up a URL (`http://...`).

-- COMMAND ----------

-- Use this regular expression to remove hashtags
select text
      ,regexp_replace(text, '#([[a-zA-ZäöüÄÖÜß]|[0-9]]+)', ' ')
from tweets_filtered

-- COMMAND ----------

-- Use this regular expression to remove user mentions
select regexp_replace(text, '@(\\w+)', ' ')
from tweets_filtered

-- COMMAND ----------

-- Use this regular expression to remove URLs from a tweet
select regexp_replace(text, 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')
from tweets_filtered

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Complete cleaning statement for tweets

-- COMMAND ----------

select id
      ,screen_name
      ,text as original_text
      ,lang
      ,created_at
     
       -- Remove white spaces at the beginning or end
      ,trim(
         -- Remove two or more subsequent white spaces
         regexp_replace(
           -- Remove special characters
             regexp_replace(
               -- Make all text lower case
               lower(
                 -- Replace user mentions
                 regexp_replace(
                   -- Replace URLs
                   regexp_replace(
                     -- Replace hashtags
                     regexp_replace(
                       -- Replace line breaks (2 different types)
                       regexp_replace(
                       regexp_replace(text, '\n', ' '),
                         '\r', ' '), '#([[a-zA-ZäöüÄÖÜß]|[0-9]]+)', ' '), 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' '), '@(\\w+)', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ')) as `text`
from tweets_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### And the final view for step 2 (cleaning)...

-- COMMAND ----------

create or replace view tweets_cleaned as
select id
      ,screen_name
      ,text as original_text
      ,lang
      ,created_at
     
       -- Remove white spaces at the beginning or end
      ,trim(
         -- Remove two or more subsequent white spaces
         regexp_replace(
           -- Remove special characters
             regexp_replace(
               -- Make all text lower case
               lower(
                 -- Replace user mentions
                 regexp_replace(
                   -- Replace URLs
                   regexp_replace(
                     -- Replace hashtags
                     regexp_replace(
                       -- Replace line breaks (2 different types)
                       regexp_replace(
                       regexp_replace(text, '\n', ' '),
                         '\r', ' '), '#([[a-zA-ZäöüÄÖÜß]|[0-9]]+)', ' '), 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' '), '@(\\w+)', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ')) as `text`
from tweets_filtered

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Tokenize
-- MAGIC ---
-- MAGIC After we filtered and cleaned our data, it's time to project a structure onto our text. In this specific example, we want to transform our text in one column into one word per row. Thus, we split our text into tokens using a space character as our separator symbol.
-- MAGIC 
-- MAGIC To split text we apply the `split()` function, which gives us an array of words as a result.

-- COMMAND ----------

select id
      ,screen_name
      ,created_at
      ,split(text, ' ') as `words`
from tweets_cleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The result from above is not what we wanted. We must take one more step to convert the array of words into *one row per word*. This is done using `explode()`:

-- COMMAND ----------

select id
      ,screen_name
      ,created_at
      ,explode(words) as word
from (      

  -- This subquery is the same from above
  select id
        ,screen_name
        ,created_at
        ,split(text, ' ') as `words`
  from tweets_cleaned
)

-- COMMAND ----------

-- We can avoid the subquery and do all in one SQL query

select id
      ,screen_name
      ,created_at
      ,explode(split(text, ' ')) as word
from tweets_cleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Alternative: Keep the index of the word
-- MAGIC ---
-- MAGIC If you need to know later at what position each word was in the original text, we can use `posexplode()` instead of `explode()`. Note that the first position is zero.

-- COMMAND ----------

select id
      ,screen_name
      ,created_at
      ,posexplode(split(text, ' ')) as (position, word)
from tweets_cleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a view for words

-- COMMAND ----------

create or replace view tweets_words as
select id
      ,screen_name
      ,created_at
      ,lang
      ,posexplode(split(text, ' ')) as (position, word)
from tweets_cleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Now we can count words:

-- COMMAND ----------

select word
      ,count(1) as `occurences`
from tweets_words
where lang = "en"
group by word
order by count(1) desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC It seems there are some words we'd rather not have in the result... 🤔

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Filter stop words
-- MAGIC ---
-- MAGIC As you can see above, some words in our result occur very often, but they don't tell us anything about the content of the tweet. In the English language those words include "and", "the", "is", and so forth. We call theses words stop words, and we don't want to include them in our result.
-- MAGIC 
-- MAGIC A pragmatic, yet naive, solution would be to use the `where` clause and filter each individual word:

-- COMMAND ----------

select word
      ,count(1) as `occurences`
from tweets_words
where lang = "en"
and word not in ('the', 'to', 'of', 'in', 'and', 'for')
group by word
order by count(1) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A better approach
-- MAGIC This naive approach leads to a very large SQL statement and to a lot of work figuring out the stop words to filter. Fortunately, there are better options. One of them is using an existing list of stop words someone else has created. There are many such lists to be found online. In the example here, we copied the list into a Google Spreadsheet which we then load automatically as a table in Databricks:

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "stopwords"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSWAaX6X1mcF7iCxFXE7dvwQHxb01L4CPlwgGPkmBYDLCsHozvANJBXs_sxlEJ37tAC-jBrZ0c7ADf2/pub?gid=0&single=true&output=csv"
-- MAGIC 
-- MAGIC var localpath = "/tmp/" + tableName + ".csv"
-- MAGIC dbutils.fs.rm("file:" + localpath)
-- MAGIC "wget -O " + localpath + " " + url !!
-- MAGIC 
-- MAGIC dbutils.fs.mkdirs("dbfs:/datasets/gsheets")
-- MAGIC dbutils.fs.cp("file:" + localpath, "dbfs:/datasets/gsheets")
-- MAGIC 
-- MAGIC sqlContext.sql("drop table if exists " + tableName)
-- MAGIC var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/datasets/gsheets/" + tableName + ".csv");
-- MAGIC 
-- MAGIC df.write.saveAsTable(tableName);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check if the import worked

-- COMMAND ----------

select *
from stopwords

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Apply the new table to our data
-- MAGIC ---
-- MAGIC Now that we have a working list of stop words, we can apply it to our tweets:

-- COMMAND ----------

select word
      ,count(1) as `occurences`
from tweets_words
where word not in (select word from stopwords)
and lang = 'en'
group by word
order by count(1) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a view for the tweets without stop words

-- COMMAND ----------

create or replace view tweets_stop as
select word
      ,count(1) as `occurences`
from tweets_words
where word not in (select word from stopwords)
and lang = 'en'
group by word

-- COMMAND ----------

select * from tweets_stop
order by occurences desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Part of speech (POS) tagging
-- MAGIC ---
-- MAGIC As a last optional step, we can enrich our words with the information about what role they play in a setence. An easy and naive approach is to use a lists of words and their possible meanings and apply it to our data.
-- MAGIC 
-- MAGIC **HINT**: There exist better approaches with statistical machine learning models. We will apply these later on, when we address NLP with Python.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC 
-- MAGIC // Choose a name for your resulting table in Databricks
-- MAGIC var tableName = "pos"
-- MAGIC 
-- MAGIC // Replace this URL with the one from your Google Spreadsheets
-- MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
-- MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQQH78wlrfTf3WCGgzenYJsUhkFjH0TLmBVfnV1nidrb6mUQdf-TSvkYk_IR2-la0OQz2WYS5CqyUm6/pub?gid=0&single=true&output=csv"
-- MAGIC 
-- MAGIC var localpath = "/tmp/" + tableName + ".csv"
-- MAGIC dbutils.fs.rm("file:" + localpath)
-- MAGIC "wget -O " + localpath + " " + url !!
-- MAGIC 
-- MAGIC dbutils.fs.mkdirs("dbfs:/datasets/gsheets")
-- MAGIC dbutils.fs.cp("file:" + localpath, "dbfs:/datasets/gsheets")
-- MAGIC 
-- MAGIC sqlContext.sql("drop table if exists " + tableName)
-- MAGIC var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/datasets/gsheets/" + tableName + ".csv");
-- MAGIC 
-- MAGIC df.write.saveAsTable(tableName);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check whether the import has worked

-- COMMAND ----------

select lower(word) as `word`, type from pos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Apply the POS table to the data
-- MAGIC ---
-- MAGIC We use a left join to apply the POS table to the data. Using a left join, we make sure that all words from our tweets are in the result, regardless of whether they are in the POS list or not. The words with no POS will have `null` for the column `pos`.

-- COMMAND ----------

select t.word
      ,p.type as `pos`
from tweets_stop t
left join pos p 
  -- Join on lower case because all words are lower case
  on p.word = lower(t.word)
