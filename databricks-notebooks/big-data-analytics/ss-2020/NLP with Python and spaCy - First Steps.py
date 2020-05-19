# Databricks notebook source
# MAGIC %md
# MAGIC # NLP with Python and spaCy - First Steps
# MAGIC ---
# MAGIC Natural Language Processing (NLP) is a field that deals with methods to let machines understand text or speech. A state-of-the-art NLP library in Python is spaCy. spaCy offers various methods to analyze text data in a way not possible with pure SQL. In this notebook, we learn the first steps with spaCy and how to perform the following tasks:<br><br>
# MAGIC 
# MAGIC - Tokenize text data
# MAGIC - Extract parts of speech, such as verbs, nouns, adjective etc.
# MAGIC - Perform stemming, that is, transforming words into their canonical form (lemma)
# MAGIC - Recognize named entities (NER), such as places, people or organizations
# MAGIC - Analyze syntactic dependencies of words in the context of sentence
# MAGIC 
# MAGIC Get an overview of spaCy here:<br><br>
# MAGIC 
# MAGIC - <a href="https://spacy.io/usage/spacy-101" target="_blank">spaCy 101</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Setup
# MAGIC ---
# MAGIC spaCy is an external Python library which me need to install first before we can use it.
# MAGIC 
# MAGIC <span style="color: orange;"><b>NOTE</b>: Since we do not add spaCy permanently to our cluster configuration, we need to run this line for every new cluster we use.</span>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install spaCy

# COMMAND ----------

dbutils.library.installPyPI("spacy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load a pre-trained model for our preferred language
# MAGIC ---
# MAGIC NLP with spaCy is mostly based on statistical machine learning models. spaCy offers pre-trained models in different sizes (complexity / features) for almost any language. Find an overview of the available models and languages here:<br><br>
# MAGIC 
# MAGIC - <a href="https://spacy.io/usage/models" target="_blank">Models & Languages (spaCy)</a>
# MAGIC - <a href="https://spacy.io/models/de" target="_blank">German - spaCy Models Documentation</a>
# MAGIC - <a href="https://spacy.io/models/en" target="_blank">English - spaCy Models Documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### German Small
# MAGIC ---
# MAGIC Smaller models are faster in execution, but they are less accurate and contain less features. For our use cases, the small model is sufficient:<br><br>
# MAGIC 
# MAGIC The following code block installs the German small model (~15 MB) on your cluster:

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install "https://github.com/explosion/spacy-models/releases/download/de_core_news_sm-2.2.5/de_core_news_sm-2.2.5.tar.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC ### German Medium
# MAGIC ---
# MAGIC The following code block installs the German medium model (~214 MB) on your cluster:

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install "https://github.com/explosion/spacy-models/releases/download/de_core_news_md-2.2.5/de_core_news_md-2.2.5.tar.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC ### English Small
# MAGIC ---
# MAGIC The following code block installs the English small model (~11 MB) on your cluster:

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install "https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-2.2.0/en_core_web_sm-2.2.0.tar.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC ### English Medium
# MAGIC ---
# MAGIC The following code block installs the English medium model (~91 MB) on your cluster:

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install "https://github.com/explosion/spacy-models/releases/download/en_core_web_md-2.2.5/en_core_web_md-2.2.5.tar.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC ### English Large
# MAGIC ---
# MAGIC The following code block installs the English large model (~790 MB) on your cluster <span style="color: red">(not recommended, takes long and blows your quota)</span>:

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install "https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-2.2.5/en_core_web_lg-2.2.5.tar.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Python on your cluster
# MAGIC ---
# MAGIC To make the installed models available, we need to restart Python on or cluster:

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Prepare our tweets
# MAGIC ---
# MAGIC When we run complex NLP operations, such as we do in the following, it is always a good idea to do some data preparation first. In particular we should make sure that our data is pre-filtered to decrease the total volume. Moreover, removing any parts that are not interesting for NLP is a good ideas to increase the result's quality. For tweets, this includes hashtags, user mentions and URLs.
# MAGIC 
# MAGIC <span style="color: orange"><b>NOTE:</b> Depending on the data the NLP model was trained on and the data it is applied to, the results can be better or worse. The model we are loading in the following works well for text from the internet, but it wasn't trained on tweets. As a consequence, it will not recognize elements such as hashtags or user mentions, and therefore likely misclassify them.</span>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter, clean, and normalize
# MAGIC ---
# MAGIC So let's create a view in which the tweets are cleaned and filtered to include only the year 2020 and tweets in English language:

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view tweets_cleaned as
# MAGIC select id
# MAGIC       ,screen_name
# MAGIC       ,text as original_text
# MAGIC       ,lang
# MAGIC       ,created_at
# MAGIC      
# MAGIC      -- Remove white spaces at the beginning or end
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
# MAGIC                          '\r', ' '), '#([[a-zA-ZäöüÄÖÜß]|[0-9]]+)', ' '), 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' '), '@(\\w+)', ' ')), '[^a-zA-ZäöüÄÖÜß]', ' '), '\ {2,}', ' ')) as `normalized_text`
# MAGIC from tweets
# MAGIC 
# MAGIC -- For simplicity, we perform the filtering from the first step here
# MAGIC where year(created_at) >= 2020
# MAGIC and lang = "en"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a permanent table for our data
# MAGIC To improve performance even more in the following, we create a permanent table from the view:

# COMMAND ----------

spark.sql("drop table if exists tweets_clean_for_nlp")
df = spark.sql('''
  select * from tweets_cleaned
''') 
df.write.saveAsTable("tweets_clean_for_nlp")

# COMMAND ----------

# MAGIC %md
# MAGIC A quick check of the result:

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tweets_clean_for_nlp

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Apply spaCy tweet by tweet with SQL (small model)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A manual test run
# MAGIC ---
# MAGIC With the following code you can play around with spaCy using your own text. Simply replace the content of the variable `text` and run the code:

# COMMAND ----------

import spacy

# Load English tokenizer, tagger, parser, named entity recognition (NER) and word vectors
nlp = spacy.load("en_core_web_md")

# Process whole documents
text = ("When Sebastian Thrun started working on self-driving cars at "
        "Google in 2007, few people outside of the company took him "
        "seriously. “I can tell you very senior CEOs of major American "
        "car companies would shake my hand and turn away because I wasn’t "
        "worth talking to,” said Thrun, in an interview with Recode earlier "
        "this week.")

# Apply nlp with spaCy
doc = nlp(text)

# Analyze syntax
print("Noun phrases:", [chunk.text for chunk in doc.noun_chunks])
print("Verbs:", [token.lemma_ for token in doc if token.pos_ == "VERB"])

for token in doc:
  print(str(token) + " : " + token.pos_ + " (" + token.lemma_ + ")")

# Find named entities, phrases and concepts
for entity in doc.ents:
    print(entity.text, entity.label_)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Extracting POS using verbs as an example
# MAGIC ---
# MAGIC Let's look at one way to apply the spaCy NLP pipeline to our tweets using SQL and a user defined function (UDF):

# COMMAND ----------

from pyspark.sql.types import ArrayType, FloatType, StringType
import spacy
nlp = spacy.load("en_core_web_sm")

def getVerbs(text):
  doc = nlp(text)
  verbs = [token.lemma_ for token in doc if token.pos_ == "VERB"]
  return verbs

spark.udf.register("getVerbs", getVerbs, ArrayType(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC We can now use the UDF in our SQL statements to extract verbs from a tweet:

# COMMAND ----------

# MAGIC %sql
# MAGIC select normalized_text
# MAGIC       ,getVerbs(normalized_text) as `verbs`
# MAGIC from tweets_clean_for_nlp

# COMMAND ----------

# MAGIC %md
# MAGIC In the same manner, we could create a function to extract other parts of speech, such as nouns or adjectives. A documentation with examples for the POS capabilites of spaCy can be found here:<br><br>
# MAGIC 
# MAGIC - <a href="https://spacy.io/usage/linguistic-features#pos-tagging" target="_blank">spaCy Linguistic Features</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## One function to extract everything
# MAGIC ---
# MAGIC Instead of writing new functions for every feature that we want to extract from our tweets, it is more efficient to create one function that includes everything we are interested in. Afterwards we can select only the columns we need for a specific analysis. 
# MAGIC 
# MAGIC The following function is an example, which you extend if needed. The function extracts the following:<br><br>
# MAGIC 
# MAGIC - Lower case tokens
# MAGIC - Regular tokens as they appear in the text (same here as we lowered everything beforehand)
# MAGIC - Tokens in canonical form (lemma)
# MAGIC - All verbs in canoncial form (lemma)
# MAGIC - All adjectives in canoncial form (lemma)
# MAGIC - All nouns in canoncial form (lemma)
# MAGIC - Noun phrases (nouns with dependent words)
# MAGIC - Persons
# MAGIC - Locations such as countries, cities etc.
# MAGIC - Organizations
# MAGIC - Events
# MAGIC - Products
# MAGIC - The subject of a tweet (can be more than one if many sentences)
# MAGIC 
# MAGIC You can find more features to include in the function here:<br><br>
# MAGIC 
# MAGIC - <a href="https://spacy.io/usage/linguistic-features" target="_blank">spaCy Linguistic Features</a>

# COMMAND ----------

from pyspark.sql.types import ArrayType, FloatType, StringType, StructType, StructField
import spacy
nlp = spacy.load("en_core_web_sm")

def apply_nlp(text):
  doc = nlp(text)
  pos_list = ["VERB", "NOUN", "ADJ", "PROPN", "ADP", "NUM", "AUX"] 

  tokens = { "token_lower" : [token.lower_ for token in doc if token.pos_ in pos_list],
             "token_text" : [token.text for token in doc if token.pos_ in pos_list],
             "token_lemma" : [token.lemma_ for token in doc if token.pos_ in pos_list],
             "verbs_lemma" : [token.lemma_ for token in doc if token.pos_ in ["VERB", "AUX"]],
             "adjectives_lemma" : [token.lemma_ for token in doc if token.pos_ == "ADJ"],
             "nouns_lemma" : [token.lemma_ for token in doc if token.pos_ in ["NOUN", "PROPN"]],
             "noun_phrases" : [chunk.text for chunk in doc.noun_chunks],
             "ner_persons" : [entity.text for entity in doc.ents if entity.label_ == "PERSON"],
             "ner_locations" : [entity.text for entity in doc.ents if entity.label_ == "GPE"],
             "ner_organizations" : [entity.text for entity in doc.ents if entity.label_ == "ORG"],
             "ner_events" : [entity.text for entity in doc.ents if entity.label_ == "EVENT"],
             "ner_products" : [entity.text for entity in doc.ents if entity.label_ == "PRODUCT"],
             "sd_subjects" : [token.text for token in doc if token.dep_ == "nsubj"]
           }
  return tokens
 
# Define the return type of the UDF
schema = StructType([
    StructField("token_lower", ArrayType(StringType()), True),
    StructField("token_text", ArrayType(StringType()), True),
    StructField("token_lemma", ArrayType(StringType()), True),
    StructField("verbs_lemma", ArrayType(StringType()), True),
    StructField("adjectives_lemma", ArrayType(StringType()), True),
    StructField("nouns_lemma", ArrayType(StringType()), True),
    StructField("noun_phrases", ArrayType(StringType()), True),
    StructField("ner_persons", ArrayType(StringType()), True),
    StructField("ner_locations", ArrayType(StringType()), True),
    StructField("ner_organizations", ArrayType(StringType()), True),
    StructField("ner_events", ArrayType(StringType()), True),
    StructField("ner_products", ArrayType(StringType()), True),
    StructField("sd_subjects", ArrayType(StringType()), True)
])

#Register the UDF so we can use it from SQL
spark.udf.register("apply_nlp", apply_nlp, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see the result of our function:

# COMMAND ----------

# MAGIC %sql
# MAGIC select original_text
# MAGIC 
# MAGIC       -- Tokens
# MAGIC       ,nlp.token_lower
# MAGIC       ,nlp.token_text
# MAGIC       ,nlp.token_lemma
# MAGIC       
# MAGIC       -- POS Tagging
# MAGIC       ,nlp.verbs_lemma
# MAGIC       ,nlp.adjectives_lemma
# MAGIC       ,nlp.nouns_lemma
# MAGIC       
# MAGIC       -- Chunks
# MAGIC       ,nlp.noun_phrases
# MAGIC       
# MAGIC       -- Named Entitiy Recognition
# MAGIC       ,nlp.ner_persons
# MAGIC       ,nlp.ner_locations
# MAGIC       ,nlp.ner_organizations
# MAGIC       ,nlp.ner_events
# MAGIC       ,nlp.ner_products
# MAGIC       
# MAGIC       -- Syntactic Dependencies
# MAGIC       ,nlp.sd_subjects
# MAGIC       
# MAGIC from (
# MAGIC   -- First apply nlp in a subquery, so that we can access individual fields
# MAGIC   select original_text
# MAGIC         ,apply_nlp(normalized_text) as `nlp`
# MAGIC   from tweets_clean_for_nlp
# MAGIC   -- For testing purpose to get faster results
# MAGIC   limit 50
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Which locations are mentioned and how often?

# COMMAND ----------

# MAGIC %sql
# MAGIC select `location`
# MAGIC        ,count(1) as `num_occurence`
# MAGIC from (
# MAGIC 
# MAGIC   -- Explode the locations array in the second subquery
# MAGIC   select explode(nlp.ner_locations) as `location`
# MAGIC   from (
# MAGIC   
# MAGIC     -- Apply nlp in first subquery
# MAGIC     select apply_nlp(normalized_text) as `nlp`
# MAGIC     from tweets_clean_for_nlp
# MAGIC     -- For testing purposes
# MAGIC     limit 2000
# MAGIC     
# MAGIC   )
# MAGIC   
# MAGIC )
# MAGIC group by `location`
# MAGIC order by `num_occurence` desc

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Apply spaCy in batches with Python (small and medium model)

# COMMAND ----------

# MAGIC %md
# MAGIC In the examples before, we used the small version of the model for the English language. There are two bigger models available, which come with higher precision and more features. The downside is that they are large and take much longer to process. 
# MAGIC 
# MAGIC Applying spaCy to every single tweet separately, as we did with the user defined function approach, is not very efficient. Instead, we can use a streaming approach by giving spaCy a batch of tweets at once. The code below uses `nlp.pipe()` to achieve that. It is based on the following steps:<br><br>
# MAGIC 
# MAGIC 1. Get the tweets into a Spark dataframe using `spark.sql()`
# MAGIC 2. Convert the Spark dataframe to a numpy array, because that's what spaCy understands
# MAGIC 3. Stream all tweets in batches using `nlp.pipe()`
# MAGIC 4. Go through the processed tweets with a FOR loop and take copy everything we need in a large array object
# MAGIC 5. When we looped through all processed tweets, convert the large array object into a Spark dataframe
# MAGIC 6. Save the dataframe as table, so we can query the whole thing with SQL again 😊

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream tweets in batches and save the result to a table

# COMMAND ----------

import numpy as np
import pandas as pd 
import spacy

# The name of the output table
tableName = "tweets_nlp_results"

# Load English model
nlp = spacy.load("en_core_web_md")

# Get the tweets into a Spark dataframe
tweets = spark.sql('''
  select id
        ,normalized_text 
  from tweets_clean_for_nlp
  limit 500  
''')

# Convert the id and text into separate numpy arrays
id_array = np.array(tweets.select("id").collect()).flatten().tolist()
text_array = np.array(tweets.select("normalized_text").collect()).flatten().tolist()

# Stream all tweets through spaCy
docs = list(nlp.pipe(text_array))

result = []

pos_list = ["VERB", "NOUN", "ADJ", "PROPN", "ADP", "NUM", "AUX"]

for num, doc in enumerate(docs, start=0):
  
    row = []
    row.append(id_array[num])
    row.append(text_array[num])

    row.append([token.text for token in doc if token.pos_ in pos_list])
    row.append([token.lower_ for token in doc if token.pos_ in pos_list])
    row.append([token.lemma_ for token in doc if token.pos_ in ["VERB", "AUX"]])
    row.append([token.lemma_ for token in doc if token.pos_ == "ADJ"])      
    row.append([token.lemma_ for token in doc if token.pos_ in ["NOUN", "PROPN"]])
    row.append([chunk.text for chunk in doc.noun_chunks])
      
    entities = []
    for entity in doc.ents:
      entities.append({ "text" : entity.text, "label" : entity.label_ })
   
    row.append(entities)
    result.append(row)

# Create a dataframe and save it as a table
df = pd.DataFrame(result) 
df = spark.createDataFrame(df, ["id", "text", "token", "token_lower", "verbs", "adjectives", "nouns", "noun_chunks", "entities"])
spark.sql("drop table if exists " + tableName)
df.write.saveAsTable(tableName)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check what the result looks like

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tweets_nlp_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bringing the NLP results back together with the rest of the metadata

# COMMAND ----------

# MAGIC %md
# MAGIC In the script above we only selected the `id` and the `normalized_text` from a tweet. In the result, we have these to columns as well, along with all the results from our NLP analysis. 
# MAGIC 
# MAGIC But what if we want to analyze the NLP columns in the context of some other metadata, say the time a tweet was created or the user? How do get that information? The answer is by joining the new table with the original tweets table via a tweet's unique id.

# COMMAND ----------

# MAGIC %sql
# MAGIC select t.created_at
# MAGIC       ,t.screen_name
# MAGIC       ,t.text
# MAGIC       ,nlp.verbs
# MAGIC from tweets t
# MAGIC inner join tweets_nlp_results nlp
# MAGIC   on t.id = nlp.id

# COMMAND ----------

# MAGIC %md
# MAGIC Now it's your turn! Looking forward to hearing from your creative applications of spaCy in the context of tweet analysis...
