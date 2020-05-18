# Databricks notebook source
# MAGIC %md
# MAGIC # Word Clouds with Python
# MAGIC ---
# MAGIC Word clouds can summarize the most frequent words in a text in a visual manner. In this notebook, wel'll use the Python library <a href="https://github.com/amueller/word_cloud" target="_blank">word_cloud</a> by scitkit-learn developer Andreas Müller to generate some word clouds based on tweets. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install the Python library
# MAGIC ---
# MAGIC With every new cluster we launch, we need to install the external libraries first. Unless we add the library to our cluster permanently.

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install wordcloud

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import all required the libraries
# MAGIC ---
# MAGIC After installing we need to import the libraries we want to use in this notebook. Maplotlib comes pre-installed with the Databrick runtime.

# COMMAND ----------

from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare the tweets
# MAGIC ---
# MAGIC We apply the steps we learned in "NLP with SQL - First Steps", so the data is clean, normalized, and doesn't contain any stop words.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and normalize

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tweets_cleaned as
# MAGIC select id
# MAGIC       ,screen_name
# MAGIC       ,text as original_text
# MAGIC       ,lang
# MAGIC       ,created_at
# MAGIC      
# MAGIC        -- Remove white spaces at the beginning or end
# MAGIC       ,trim(
# MAGIC          -- Remove two or more subsequent white spaces
# MAGIC          regexp_replace(
# MAGIC            -- Remove special characters
# MAGIC              regexp_replace(
# MAGIC                -- Make all text lower case
# MAGIC                lower(
# MAGIC                  -- Replace user mentions
# MAGIC                  regexp_replace(
# MAGIC                    -- Replace URLs
# MAGIC                    regexp_replace(
# MAGIC                      -- Replace hashtags
# MAGIC                      regexp_replace(
# MAGIC                        -- Replace line breaks (2 different types)
# MAGIC                        regexp_replace(
# MAGIC                        regexp_replace(text, '\n', ' '),
# MAGIC                          '\r', ' '), '#([[a-zA-ZäöüÄÖÜß]|[0-9]]+)', ' '), 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' '), '@(\\w+)', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ')) as `text`
# MAGIC from tweets_filtered
# MAGIC -- For this example we only want English tweets
# MAGIC where lang = "en"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tokenize

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tweets_words as
# MAGIC select id
# MAGIC       ,screen_name
# MAGIC       ,created_at
# MAGIC       ,lang
# MAGIC       ,text
# MAGIC       ,posexplode(split(text, ' ')) as (position, word)
# MAGIC from tweets_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter stop words
# MAGIC ---
# MAGIC Filtering out the stop words is important for word clouds. Otherwise you will only see words such as "is", "are", "and" in the cloud.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tweets_stop as
# MAGIC select word
# MAGIC from tweets_words
# MAGIC where word not in (select word from stopwords)
# MAGIC and lang = 'en'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare the data for the word cloud
# MAGIC ---
# MAGIC The word cloud library expects the words to be in one row. Because we have just split the words into many rows to filter the stopwords, we must now put them back together into one row with a large chunk of text.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the words
# MAGIC ---
# MAGIC Before we proceed, let's check if we are happy with the list that contains the most frequent words. If not, we can extend our stopword list or filter them out below.

# COMMAND ----------

# MAGIC %sql
# MAGIC select word
# MAGIC       ,count(1) as `count`
# MAGIC from tweets_stop
# MAGIC -- Filter out some words manually
# MAGIC where word not in ('not')
# MAGIC group by word
# MAGIC order by `count` desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take the top 50 words for the word cloud
# MAGIC ---
# MAGIC For the word cloud, we must limit our number of words for two reasons:<br><br>
# MAGIC 
# MAGIC 1. The word cloud will become hard to read if there are too many words in it. 
# MAGIC 2. There is a technical limit on how many words can be used to generate a word cloud.
# MAGIC 
# MAGIC Let's use the query from the check above and limit it to the top 50 words:

# COMMAND ----------

# MAGIC %sql
# MAGIC select word
# MAGIC       ,count(1) as `count`
# MAGIC from tweets_stop
# MAGIC where word not in ('not')
# MAGIC group by word
# MAGIC order by `count` desc
# MAGIC -- Limit the result to 50 rows (words in this case)
# MAGIC limit 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a view for the top 50 words
# MAGIC ---
# MAGIC For convenience, let's create a view for that:

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tweets_top_50 as
# MAGIC   select word
# MAGIC   from tweets_stop
# MAGIC   where word not in ('not')
# MAGIC   group by word
# MAGIC   order by count(1) desc
# MAGIC   limit 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concatenate the single word into one large chunk
# MAGIC ---
# MAGIC Now it's time to concatenate all words from our top 50 into one row containing a large chunk of text. To achieve this, we first collect the single words into one large array using `collect_list()`. Then, we apply the `array_join()` function to take every item in the array and concatenate it with t all other items using a space as the separator symbol:

# COMMAND ----------

# MAGIC %sql
# MAGIC select array_join(collect_list(word), ' ') as `text`
# MAGIC from (
# MAGIC   select word
# MAGIC   from tweets_stop
# MAGIC   where word in (select word from tweets_top_50)
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create a view for the final result

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tweets_word_cloud as
# MAGIC   select array_join(collect_list(word), ' ') as `text`
# MAGIC   from (
# MAGIC     select word
# MAGIC     from tweets_stop
# MAGIC     where word in (select word from tweets_top_50)
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC <span style="color:orange"><b>NOTE</b>: If we have a lot of data and queries are slow, can instead store the result in a permanent table. This is what is done in this notebook, as it will speed up word cloud creation:</span>

# COMMAND ----------

df = spark.sql('''
 select array_join(collect_list(word), ' ') as `text`
 from (
    select word
    from tweets_stop
    where word in (select word from tweets_top_50)
 )
''')

df.write.saveAsTable("tweets_word_cloud_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the data in a Spark data frame in Python
# MAGIC ---
# MAGIC Once we are happy with our data, we need to load it into a Spark data frame, so the word cloud library can access it:

# COMMAND ----------

# Create a variable text that contains our text for the word cloud
# Change the table/view you are selecting from depending on whether you created a table or not
text = spark.sql('''
  select text from tweets_word_cloud_table
''')

# To check, print the first line, which contains everything
text.take(1)[0].text

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate the word cloud
# MAGIC ---
# MAGIC The data is ready, now it's time to generate the word cloud. The word cloud has a number of arguments that we can use to change the appearance of the cloud. In the example below we use the following:<br><br>
# MAGIC 
# MAGIC - `scale=10` - This increases the resolution of the resulting image. If you lower the value, the resolution will be lower
# MAGIC - `background_color="white"` - Set the background of the resulting image
# MAGIC - `stopwords=my_stopwords` - Filter out additional stopwords before generating. This can be used for last minute fixes
# MAGIC - `random_state=1` - If we set this value, we get the same cloud if the input stays the same and we run the code again. Otherwise we will get a random arrangement everytime we run the code.
# MAGIC 
# MAGIC We then pass our data frame's only column to the `generate()` function. This column contains the whole text we prepared above. The lines after that do the following:<br><br>
# MAGIC 
# MAGIC - `figsize(20, 20)` - Set the resulting image to 20 x 20 inches
# MAGIC - `plt.axis(off)` - Remove the x and y-axis. The word cloud uses Matplotlib under the hood, and a plot by default contains both axes.
# MAGIC - `plt.imshow()` - Show the generated word cloud image
# MAGIC 
# MAGIC The commented lines at the bottom show you how to export the image as a PNG-file and store in your local Databricks file system. Once it is there, you can use a public URL to access it.

# COMMAND ----------

# In python we can also filter stop words 
my_stopwords = set(STOPWORDS)
#my_stopwords.add("und")

# Generate a word cloud image
wordcloud = WordCloud(
                      scale=10
                     ,background_color="white"
                     ,stopwords=my_stopwords
                     ,random_state=1 # Make sure the output is always the same for the same input
             ).generate(text.take(1)[0].text)

# Display the generated image the matplotlib way:
plt.figure(figsize=(20,20))
plt.axis("off")
plt.imshow(wordcloud, interpolation='bilinear')

# Optional: You can directly save the visualization in your Databricks account and access the file using a public URL
# dbutils.fs.mkdirs("dbfs:/FileStore/viz")
# plt.savefig('/dbfs/FileStore/viz/word_cloud.png')

# Public URL example: https://community.cloud.databricks.com/files/viz/word_cloud.png?o=7817603968412618


# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the image export in Databricks
# MAGIC ---
# MAGIC With the `displayHTML` function, we can show any image in a Databricks notebook. This example displays the word cloud we created above:

# COMMAND ----------

displayHTML("<img src='https://community.cloud.databricks.com/files/viz/word_cloud.png?o=7817603968412618' width='50%'>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Change the color scheme of the word cloud
# MAGIC ---
# MAGIC We can use pre-defined color maps from Matplotlib to change the appearance of our word cloud. Find all color maps on the <a href="https://matplotlib.org/3.2.1/tutorials/colors/colormaps.html" target="_blank">Matplotlib documentation website</a>:

# COMMAND ----------

# Generate a word cloud image
wordcloud = WordCloud(
                      scale=10
                     ,background_color="white"
                     ,stopwords=my_stopwords
                     ,random_state=1 # Make sure the output is always the same for the same input
                     ,colormap="inferno"
             ).generate(text.take(1)[0].text)

# Display the generated image the matplotlib way:
plt.figure(figsize=(20,20))
plt.axis("off")
plt.imshow(wordcloud, interpolation='bilinear')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tweak the word cloud: Add a circle mask

# COMMAND ----------

import numpy as np

# Define the circle mask
# Example found here: http://amueller.github.io/word_cloud/auto_examples/single_word.html#sphx-glr-auto-examples-single-word-py
x, y = np.ogrid[:300, :300]
mask = (x - 150) ** 2 + (y - 150) ** 2 > 130 ** 2
mask = 255 * mask.astype(int)

# Generate a word cloud image
wordcloud = WordCloud(
                     scale=10
                     ,background_color="white"
                     ,mask=mask
                     ,stopwords=stopwords
                     ,random_state=1
                     ,colormap="coolwarm"
            ).generate(text.take(1)[0].text)

# Display the generated image the matplotlib way:
plt.figure(figsize=(20,20))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")

# COMMAND ----------

# MAGIC %md
# MAGIC ## More ways to change your word cloud
# MAGIC ---
# MAGIC The word cloud library gives us many options to further tweak the word cloud. Find all parameters and their meaning at the <a href="http://amueller.github.io/word_cloud/generated/wordcloud.WordCloud.html" target="_blank">official documentation.</a>
