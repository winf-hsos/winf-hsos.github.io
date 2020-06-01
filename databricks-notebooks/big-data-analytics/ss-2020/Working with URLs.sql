-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Working with URLs
-- MAGIC ---
-- MAGIC Data often contains URLs that contain useful information for analysis. For example, we could ask which news sites are most often cited by a specific group of users. For that, we need to be able to handle URLs and extract parts of interest from them.
-- MAGIC 
-- MAGIC In this notebook, we'll learn:<br><br>
-- MAGIC 
-- MAGIC 1. How to access the URL field in twitter data
-- MAGIC 2. How to get the real URL behind a shortened link (such as bit.ly)
-- MAGIC 3. How to extract parts from a URL using `parse_url()`
-- MAGIC 4. How to retrieve information from a website behind a URL, such as the title

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. First steps with URLs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Only tweets with at least one URL
-- MAGIC Let's start easy and filter only those tweets that actually contain at least one URL:

-- COMMAND ----------

select urls from tweets
where size(urls) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ### Only tweets with at more than one URL
-- MAGIC Let's now see if there are tweets with more than one URL:

-- COMMAND ----------

select urls from tweets
where size(urls) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you can see, there are some tweets with 2 or even more URLs. You may also have noticed that the URLs are stored in a data structure called an *array*. Refer to the notebook about how to work with array for more details: <a href="https://winf-hsos.github.io/databricks-notebooks/big-data-analytics/ss-2020/Arrays%20with%20SQL.html" target="_blank">Arrays with SQL</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ### Distribution of the number of URLs in tweets
-- MAGIC 
-- MAGIC Let's see how the distribution of URLs in our tweet is:

-- COMMAND ----------

select size(urls) as `Number of URLs`
      ,count(1) as `Number of Tweets`
from tweets
group by size(urls)
order by size(urls) asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can see that most tweets contain no URL and almost as many contain one. Tweets with more than one URL are clearly an exception.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Get one URL per row
-- MAGIC 
-- MAGIC As usual, we can explode the contents of an array into single lines to better analyze it:
-- MAGIC 
-- MAGIC <span style="color: green"><b>NOTE</b>: When we use explode(), we can leave out the filter to include only tweets with at least one URL. When we apply explode() to an empty array, there is no result.</span>

-- COMMAND ----------

select explode(urls) as `url`
from tweets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can see that each array item is in turn an object with three fields:<br><br>
-- MAGIC 
-- MAGIC - `clean_url` - The URL without any URL parameters, the part after the "?"
-- MAGIC - `expanded_url` - The URL including the URL parameters
-- MAGIC - `host` - The host of the URL
-- MAGIC 
-- MAGIC We can directly use the `host` field to find out which website was most often cited:

-- COMMAND ----------

select url.host as `Host`
      ,count(1) as `Number cited`
from (
  select explode(urls) as `url`
  from tweets
)
group by url.host
order by `Number cited` desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <span style="color: red"><b>CAREFUL:</b></span> Hosts bit.ly or dlvr.it are not real hosts. They are so called link shorteners. In order to find the real website behind theses URLs, we need to actually call them. Follow along to see how that works.
-- MAGIC 
-- MAGIC Moreover, the host twitter.com is usually due to a quote or a retweet of another tweet. So you might want to filter those.

-- COMMAND ----------

select * from tweets
where array_contains(urls.host, 'bit.ly')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Find out the real URL behind a shortened link

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To get a complete picture, we want to include URLs and the hosts hiding behind a shortened link, too. As often, using Python helps!

-- COMMAND ----------

-- MAGIC  %python
-- MAGIC import requests
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC 
-- MAGIC def unshorten_url(url):
-- MAGIC   try:
-- MAGIC     return requests.head(url, allow_redirects=True, timeout = 2).url
-- MAGIC   except requests.exceptions.ReadTimeout:
-- MAGIC         print("READ TIMED OUT -", url)
-- MAGIC   except requests.exceptions.ConnectionError:
-- MAGIC         print( "CONNECT ERROR -", url)
-- MAGIC   except eventlet.timeout.Timeout:
-- MAGIC         print( "TOTAL TIMEOUT -", url)
-- MAGIC   except requests.exceptions.RequestException:
-- MAGIC         print( "OTHER REQUESTS EXCEPTION -", url)
-- MAGIC 
-- MAGIC # Register as SQL user defined function
-- MAGIC spark.udf.register("unshorten_url", unshorten_url, StringType())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using the function in Python (the faster way)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's try it with a known URL:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC unshorten_url("http://dlvr.it/RWXQ4m")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This works! So let's get all of our URLs from shortener services into a Python array and apply the function for all of them:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC import numpy as np
-- MAGIC import pandas as pd
-- MAGIC table_name = "unshortened_urls"
-- MAGIC 
-- MAGIC # Get all URLs from known link shortener services (limit for example)
-- MAGIC urls_df = spark.sql('''
-- MAGIC   select id
-- MAGIC         ,url.clean_url as url
-- MAGIC   from tweets_urls
-- MAGIC   where url.host in ("bit.ly", "ow.ly", "tinyurl.com", "dlvr.it")
-- MAGIC   limit 20
-- MAGIC ''')
-- MAGIC 
-- MAGIC shortened_urls_array = np.array(urls_df.select("url").collect()).flatten()
-- MAGIC id_array = np.array(urls_df.select("id").collect()).flatten()
-- MAGIC 
-- MAGIC result = []
-- MAGIC 
-- MAGIC for num, u in enumerate(shortened_urls_array, start=0):
-- MAGIC   unshortened_url = unshorten_url(u)
-- MAGIC   id = id_array[num]
-- MAGIC   result.append({ "id": id, "original_url": shortened_urls_array[num], "unshortened_url": unshortened_url })
-- MAGIC   print("Finished " + str(num + 1) + " URLs.")
-- MAGIC 
-- MAGIC #print(result)
-- MAGIC   
-- MAGIC # Create a dataframe and save it as a table
-- MAGIC df = pd.DataFrame(result) 
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC spark.sql("drop table if exists " + table_name)
-- MAGIC df.write.saveAsTable(table_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's check the result:

-- COMMAND ----------

select * from unshortened_urls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using a UDF with SQL (slow way)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC And now from SQL! The following statement first explodes all URLs and then applied the new UDF `unshorten_url()` for all known URL shorteners. Be careful with large data! The function performs a reqeust over the web, so running this with a large number of records may take some time. Make sure you persist your result for further analysis:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("drop table if exists tweets_urls")
-- MAGIC df = spark.sql('''
-- MAGIC    select id
-- MAGIC          ,explode(urls) as `url`
-- MAGIC    from tweets
-- MAGIC ''')
-- MAGIC df.write.saveAsTable("tweets_urls")

-- COMMAND ----------

-- This takes quite long (better to use Python approach from above)
select unshorten_url(url.clean_url)
from tweet_urls
tablesample (10 rows)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Extract parts from URL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The URL by itself is difficult to analyze, because it is rather unstructured. It is the different parts, such as the host or the specific article, that are of interest to us. Let's see how we can extract those parts:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extract the host from a URL

-- COMMAND ----------

select parse_url(unshortened_url, 'HOST') 
from unshortened_urls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extract the URL query parameters

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Can't think of a good use case, but for the sake of completeness:

-- COMMAND ----------

-- Get only the query parameter part from the URL
select parse_url(unshortened_url, 'QUERY') 
from unshortened_urls

-- COMMAND ----------

-- Get only a specific URL parameter value from the URL
select parse_url(unshortened_url, 'QUERY', 'utm_source') 
from unshortened_urls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Remove the "www" from a URL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When comparing URL hosts, it's a good idea to remove the 'www' part, because usually the same website can be reached with or without it. So we will likely encounter both versions in our data, but we want to count them as one and the same:

-- COMMAND ----------

select parse_url(unshortened_url, 'HOST') 
from unshortened_urls

-- COMMAND ----------

select regexp_replace(parse_url(unshortened_url, 'HOST'), '^(www.)', '') as `No www`
      ,parse_url(unshortened_url, 'HOST') as `Original`
from unshortened_urls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Get information from the website behind a URL (the title as an example)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It would be great if we could extend our twitter data with data from the websites people share on Twitter! We can do this, just as we can get the links behind shortened URLs, using a request to that page and parsing the content we get back. This takes some time for a large number of URLs, but it might be worth it. Here's an example, feel free to extend to other parts:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A manual example with built-in Python libraries

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import re
-- MAGIC import requests
-- MAGIC 
-- MAGIC url = "https://www.bild.de/sport/fussball/fussball/bild-beantwortet-die-wichtigsten-fragen-drohen-fan-demos-beim-derby-70639192.bild.html?utm_source=dlvr.it&utm_medium=twitter"
-- MAGIC n = requests.get(url)
-- MAGIC al = n.text.replace("\r","").replace("\n","").replace("\t", "")
-- MAGIC d = re.search('<title[^>]*>(.*?)</title>', al, re.IGNORECASE)
-- MAGIC 
-- MAGIC d.group(1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Get the website's title from tweet URLs
-- MAGIC Similar to the unshortening of URLs, getting the title for a bulk of URLs works the same:

-- COMMAND ----------

select * from unshortened_urls

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import re
-- MAGIC import requests
-- MAGIC import numpy as np
-- MAGIC import pandas as pd
-- MAGIC table_name = "website_titles"
-- MAGIC 
-- MAGIC # Get all URLs from the result above
-- MAGIC urls_df = spark.sql('''
-- MAGIC   select id
-- MAGIC         ,original_url
-- MAGIC         ,unshortened_url
-- MAGIC   from unshortened_urls
-- MAGIC ''')
-- MAGIC 
-- MAGIC unshortened_urls_array = np.array(urls_df.select("unshortened_url").collect()).flatten()
-- MAGIC original_urls_array = np.array(urls_df.select("original_url").collect()).flatten()
-- MAGIC id_array = np.array(urls_df.select("id").collect()).flatten()
-- MAGIC 
-- MAGIC result = []
-- MAGIC 
-- MAGIC # This function extracts the title from URLs by calling them and parsing the resulting HTML document
-- MAGIC def get_title_from_url(url):
-- MAGIC   if url is None:
-- MAGIC     return None
-- MAGIC   
-- MAGIC   n = requests.get(url)
-- MAGIC   
-- MAGIC   # This regular exression works to get titles
-- MAGIC   al = n.text.replace("\r","").replace("\n","").replace("\t", "")
-- MAGIC   d = re.search('<title[^>]*>(.*?)</title>', al, re.IGNORECASE)
-- MAGIC 
-- MAGIC   # Sometimes we don't get a title, return nothing then (null in the database)
-- MAGIC   if d is not None:
-- MAGIC     return d.group(1)
-- MAGIC   else: 
-- MAGIC     return None
-- MAGIC 
-- MAGIC for num, u in enumerate(unshortened_urls_array, start=0):
-- MAGIC   title = get_title_from_url(u)
-- MAGIC   id = id_array[num]
-- MAGIC   result.append({ "id": id, "original_url": original_urls_array[num], "unshortened_url": unshortened_urls_array[num], "title": title })
-- MAGIC   print("Finished " + str(num + 1) + " titles.")
-- MAGIC   
-- MAGIC # Create a dataframe and save it as a table
-- MAGIC df = pd.DataFrame(result) 
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC spark.sql("drop table if exists " + table_name)
-- MAGIC df.write.saveAsTable(table_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is the result:

-- COMMAND ----------

select * from website_titles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Voilá! We just generated new text data to analyze 😊
