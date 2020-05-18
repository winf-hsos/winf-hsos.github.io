# Databricks notebook source
# MAGIC %md
# MAGIC # Visualizations with Python
# MAGIC ---
# MAGIC Visualizations are crucial to understanding complex data. They can quickly reveal developments, contrast quantities, and show patterns in data. Databricks offers some <a href="https://docs.databricks.com/notebooks/visualizations/index.html" target="_blank">capability to visualize results</a>. This is great for a quick visual glance at the data. However, for deeper analysis we need something more flexible. Popular choices for advanced visualizations witj Python are the libraries <a href="https://matplotlib.org/" target="_blank">Matplotlib</a> and <a href="https://seaborn.pydata.org/" target="_blank">seaborn</a>. The latter builds upon Matplotlib and makes visualizations even easier and somewhat prettier.
# MAGIC 
# MAGIC In this notebook, we look at seaborn as the easier, but at the same time less flexible, way to create visualizations. It is easier because it requires less code to create a visualization. A set of tutorials for different use cases can be found <a href="https://seaborn.pydata.org/tutorial.html" target="_blank">here</a>.
# MAGIC 
# MAGIC As examples, we'll introduce the following chart types in this notebook:<br><br>
# MAGIC 
# MAGIC - A: Visualize categorical data
# MAGIC   - Bar plots
# MAGIC   - Scatter plots (for categorical data)
# MAGIC   - Box plots
# MAGIC - B: Visualize relationships in data
# MAGIC   - Scatter plot
# MAGIC   - Scatter plot with bivariate and univariate distributions
# MAGIC - C: Visualize development and trends in data
# MAGIC   - Line plot
# MAGIC   - Line plot with multiple series
# MAGIC 
# MAGIC <p style="color:orange;">**NOTE:** This is a Python notebook, which means the default code expected in a code block is Python. If you need to write SQL code, you can put `%sql` in the first line of the code block.</p>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import and configure seaborn

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's enable notebooks to show plots right below our code block (inline).

# COMMAND ----------

# MAGIC %matplotlib inline

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's import the required libraries. Additionally, we set the plots to apply an autolayot mechanism to prevent cases where parts of the image are cut off when exporting the plot as an image file.

# COMMAND ----------

# Import seaborn and matplotlib
import seaborn as sns
import matplotlib.pyplot as plt

# Make sure the plots are layouted properly when exported
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

# Set some syle, see https://seaborn.pydata.org/tutorial/aesthetics.html
#sns.set_style("darkgrid")
#sns.set_style("whitegrid")
sns.set_style("ticks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A: Visualize categorical data
# MAGIC ---
# MAGIC Find a tutorial on visualizing categorical data here: <a href="https://seaborn.pydata.org/tutorial/categorical.html" target="_blank">seaborn tutorial on plotting categorical data</a>
# MAGIC 
# MAGIC We'll look at examples for the following plots in this section:<br><br>
# MAGIC 
# MAGIC - Bar plots
# MAGIC - Scatter plots (for categorical data)
# MAGIC - Box plots

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bar plots
# MAGIC ---
# MAGIC Bar plots are a common tool to visualize categorical data and quantities. Let's start with a simple use case: We want to create a bar plot with the number of tweets in 2020 of our 50 users with the fewest tweets in the data set. The following SQL gives us the information.
# MAGIC 
# MAGIC Find detailed documentation on categorical bar plots here:<br><br>
# MAGIC 
# MAGIC - <a href="https://seaborn.pydata.org/tutorial/categorical.html#bar-plots" target="_blank">Bar plots</a>
# MAGIC - <a href="https://seaborn.pydata.org/generated/seaborn.barplot.html#seaborn.barplot" target="_blank">seaborn.catplot API documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the data for the plot with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select screen_name
# MAGIC       ,count(1) as `num_tweets`
# MAGIC from tweets
# MAGIC where year(created_at) = 2020
# MAGIC group by screen_name
# MAGIC order by num_tweets asc
# MAGIC limit 50

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Pandas dataframe in Python from the SQL statement
# MAGIC ---
# MAGIC To use the result from the above SQL statement in Python and in seaborn, we can use the `spark.sql()` function to execute an SQL statement and store the result as a Spark dataframe. We name the dataframe `tweets_df`. To allow a multi-line SQL statement we can use the triple-single-quote notation `'''` as shown in the code block below.
# MAGIC 
# MAGIC Because seaborn is a Python library and works with Pandas dataframes, we need to convert the Spark dataframe into a Pandas dataframe using `toPandas()`.

# COMMAND ----------

# Get the data with SQL
tweets_df = spark.sql('''
select screen_name
      ,count(1) as `num_tweets`
from tweets
where year(created_at) = 2020
group by screen_name
order by num_tweets asc
limit 50
''').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the plot with seaborn

# COMMAND ----------

sns.set(style="darkgrid")
# Create a categorical plot of the type bar plot

# We set the height to 5 units and the aspect ration to 3. This makes the plot 3 times wider than high.
# The x and y attributes specify which columns from our dataframe should be used on the 2 axis
ax = sns.catplot(x="screen_name"
                ,y="num_tweets"
                ,kind="bar"
                ,color="#009ee3"
                ,data=tweets_df
                ,height=6
                ,aspect=3
                )

# Set a speaking label for both axis
ax.set(xlabel='Benutzer', ylabel='Anzahl Tweets')

# Roatate and align the labes to make them readable
ax.set_xticklabels(rotation=45, horizontalalignment='right')

# Optional: Save the figure in your Databricks File System
# You need to create a subdirectory the first time

# dbutils.fs.mkdirs("dbfs:/FileStore/viz")
plt.savefig('/dbfs/FileStore/viz/tweets_by_user.svg')

# You can download the image file by pasting the following URL into your browser (adjust file name and o-value)
# https://community.cloud.databricks.com/files/viz/<image-file-name>?o=<o-value-from-your-databricks-url>

# Example: https://community.cloud.databricks.com/files/viz/tweets_by_user.svg?o=7817603968412618

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the saved file (or any image) in Databricks

# COMMAND ----------

displayHTML("<img src='https://community.cloud.databricks.com/files/viz/tweets_by_user.svg?o=7817603968412618' width='50%'>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scatter plots (for categorical data)
# MAGIC ---
# MAGIC We can also uss scatter plots with categorical data. Let's plot the number of retweets for every tweet of a user and use the user as a category.
# MAGIC 
# MAGIC Find detailed documentation on categorical scatter plots here:<br><br>
# MAGIC 
# MAGIC - <a href="https://seaborn.pydata.org/tutorial/categorical.html#categorical-scatterplots" target="_blank">Categorical scatter plots</a>
# MAGIC - <a href="https://seaborn.pydata.org/generated/seaborn.catplot.html#seaborn.catplot" target="_blank">seaborn.catplot API documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the data for the plot with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from (
# MAGIC select screen_name
# MAGIC       ,retweet_count 
# MAGIC from tweets
# MAGIC where is_retweet = false
# MAGIC and retweet_count > 0
# MAGIC )

# COMMAND ----------

retweets_count_df = spark.sql('''
select screen_name
      ,retweet_count 
from tweets
where is_retweet = false
and retweet_count > 0
''').toPandas()

# COMMAND ----------

# Create a categorical plot of the type scatter plot
ax = sns.catplot(x="screen_name"
                ,y="retweet_count"
                #,kind="swarm"
                ,color="#009ee3"
                ,data=retweets_count_df
                ,height=6
                ,aspect=2
                )

# Set a speaking label for both axis
ax.set(xlabel='Benutzer', ylabel='Anzahl Retweets')

# Roatate and align the labes to make them readable
ax.set_xticklabels(rotation=45, horizontalalignment='right')

#plt.savefig('/dbfs/FileStore/viz/tweets_by_user.svg')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Box plots
# MAGIC ---
# MAGIC The above visualization lends itself to be shown as box plots too. Let's see how we can achieve that.
# MAGIC 
# MAGIC Find detailed documentation on box plots here:<br><br>
# MAGIC 
# MAGIC - <a href="https://seaborn.pydata.org/tutorial/categorical.html#boxplots" target="_blank">Distributions of observations within categories (Box Plot)</a>
# MAGIC - <a href="https://seaborn.pydata.org/generated/seaborn.boxplot.html#seaborn.boxplot" target="_blank">seaborn.boxplot API documentation</a>

# COMMAND ----------

ax = sns.catplot(x="screen_name"
           ,y="retweet_count"
           ,kind="box"
           ,data=retweets_count_df
           ,height=6
           ,aspect=3
           #,color="green"
           ,showfliers=False) # Hide outliers

# Cut the y-axis at 200
ax.set(ylim=(0, 200))

# Set a speaking label for both axis
ax.set(title= "Verteilung Anzahl Retweets pro Benutzer", xlabel='Benutzer', ylabel='Anzahl Retweets')

# Roatate and align the labes to make them readable
ax.set_xticklabels(rotation=45, horizontalalignment='right')

# COMMAND ----------

# MAGIC %md
# MAGIC ## B: Visualize relationships in data
# MAGIC Find a tutorial on visualizing statistical relationships in data here: <a href="https://seaborn.pydata.org/tutorial/relational.html" target="_blank">seaborn tutorial on visualizing relationships in data</a>
# MAGIC 
# MAGIC We'll look at examples for the following plots in this section:<br><br>
# MAGIC 
# MAGIC - Scatter plot
# MAGIC - Scatter plot with bivariate and univariate distributions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scatter plot
# MAGIC ---
# MAGIC Scatter plots are mostly used to plot two variables against each other to see whether there exist a certain pattern. More precise, we often look for correlations between two variables. In the following example we want to see if there is a correlation between the times a tweet was liked (or favorited) and its number of retweets.
# MAGIC 
# MAGIC Find detailed documentation on scatter plots here:<br><br>
# MAGIC 
# MAGIC - <a href="https://seaborn.pydata.org/tutorial/relational.html#relating-variables-with-scatter-plots" target="_blank">Relating variables with scatter plots</a>
# MAGIC - <a href="https://seaborn.pydata.org/generated/seaborn.relplot" target="_blank">seaborn.relplot API documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the data
# MAGIC ---
# MAGIC The following statement selects the two relevant columns for our x and y axis, along with the user's screen name. The screen name will serve as a third dimension and we'll use it to color the dots.

# COMMAND ----------

# MAGIC %sql
# MAGIC select screen_name as `Twitter User`
# MAGIC       ,retweet_count
# MAGIC       ,favorite_count 
# MAGIC from tweets
# MAGIC -- Inlcude only original tweets from these users
# MAGIC where is_retweet = false
# MAGIC -- We want to compare these three politicians in our plot
# MAGIC and screen_name in ('c_lindner', 'SWagenknecht', 'DoroBaer')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the data into a data frame
# MAGIC ---
# MAGIC For Python libraries we must load the data into a data frame. We can do this using our SQL-statement and using it as an argument for the `spark.sql()` function:

# COMMAND ----------

ret_fav_df = spark.sql('''
select screen_name as `Twitter User`
      ,retweet_count
      ,favorite_count 
from tweets
where is_retweet = false
and screen_name in ('c_lindner', 'SWagenknecht', 'DoroBaer')
''').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the plot

# COMMAND ----------

ax = sns.relplot(x="favorite_count"
                ,y="retweet_count"
                ,hue="Twitter User"
                ,height=10
                ,aspect=1
                ,data=ret_fav_df
                );

# Cut the y-axis at 200 (retweets) and the x-axis at 4000 (likes)
ax.set(ylim=(0, 750), xlim=(0,4000))

# Set a speaking label for both axis
ax.set(title= "Number Likes vs. Retweets", xlabel='Number Likes', ylabel='Number Retweets')

# COMMAND ----------

# MAGIC %md
# MAGIC We can also employ the size of dot to plot a third dimension. Let's make tweets bigger if they contain more hashtags:

# COMMAND ----------

ret_fav_df = spark.sql('''
select screen_name as `Twitter User`
      ,retweet_count
      ,favorite_count
      ,size(hashtags) as `Number Hashtags`
from tweets
where is_retweet = false
and screen_name in ('c_lindner', 'SWagenknecht', 'DoroBaer')
''').toPandas()

# COMMAND ----------

ax = sns.relplot(x="favorite_count"
                ,y="retweet_count"
                ,hue="Twitter User"
                ,size="Number Hashtags"
                ,sizes=(15, 200)
                ,height=10
                ,aspect=1
                ,data=ret_fav_df
                )

# Cut the y-axis at 200 (retweets) and the x-axis at 4000 (likes)
ax.set(ylim=(0, 750), xlim=(0,4000))

# Set a speaking label for both axis
ax.set(title= "Number Likes vs. Retweets", xlabel='Number Likes', ylabel='Number Retweets')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scatter plot with bivariate and univariate distributions
# MAGIC ---
# MAGIC We can create combined plots to show not just the relationship between x and y, but also their own distribution as a histogram. All in one plot. <a href="https://seaborn.pydata.org/tutorial/regression.html#plotting-a-regression-in-other-contexts" target="_blank">Find details on the official seaborn documentation website.</a>
# MAGIC 
# MAGIC For this plot, we need to narrow down the data to just one user:

# COMMAND ----------

# MAGIC %md
# MAGIC #### Narrow down the data to just one user

# COMMAND ----------

ret_fav_df = spark.sql('''
select screen_name as `Twitter User`
      ,retweet_count
      ,favorite_count
      ,size(hashtags) as `Number Hashtags`
from tweets
where is_retweet = false
and screen_name in ('SWagenknecht')
''').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the plot
# MAGIC ---
# MAGIC To create the plot containing both distributions, the joint and one for each of the variables, we use `sns.jointplot()`:

# COMMAND ----------

ax = sns.jointplot(x="favorite_count"
                  ,y="retweet_count"
                  ,kind="reg"
                  ,height=10
                  ,color="#009ee3"
                  #,xlim=(-500,5000)
                  #,ylim=(-250,1250)
                  ,data=ret_fav_df
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ## C: Visualize development and trends in data
# MAGIC Find a tutorial on visualizing developments and trends in data here: <a href="https://seaborn.pydata.org/tutorial/relational.html#emphasizing-continuity-with-line-plots" target="_blank">seaborn tutorial on visualizing continuity in data</a>
# MAGIC 
# MAGIC We'll look at examples for the following plots in this section:<br><br>
# MAGIC 
# MAGIC - Line plot
# MAGIC - Line plot with multiple series

# COMMAND ----------

# MAGIC %md
# MAGIC ### Line plot
# MAGIC ---
# MAGIC Line plots are effective when we want to visualize developments or trends in data.
# MAGIC 
# MAGIC Find detailed documentation on line plots here:<br><br>
# MAGIC 
# MAGIC - <a href="https://seaborn.pydata.org/tutorial/relational.html#emphasizing-continuity-with-line-plots" target="_blank">Emphasizing continuity with line plots</a>
# MAGIC - <a href="https://seaborn.pydata.org/generated/seaborn.relplot" target="_blank">seaborn.relplot API documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the data for the plot

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(created_at, 'YYYY-MM-dd') as `Day`
# MAGIC       ,count(1) as `Number Tweets`
# MAGIC from tweets
# MAGIC where year(created_at) = 2020
# MAGIC and array_contains(hashtags, 'corona')
# MAGIC group by `Day` 
# MAGIC order by `Day`

# COMMAND ----------

# MAGIC %md
# MAGIC As always, we need to load the data into a dataframe:

# COMMAND ----------

df_tweet_development = spark.sql('''
  select date_format(created_at, 'yyyy-MM-dd') as `Day`
        ,count(1) as `Number Tweets`
  from tweets
  where year(created_at) = 2020
  and array_contains(hashtags, 'corona')
  group by `Day` 
  order by `Day`
''').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the line plot from the data

# COMMAND ----------

ax = sns.relplot(x="Day"
               ,y="Number Tweets"
               ,kind="line"
               ,height=6
               ,aspect=4
               ,color="#009ee3"
               ,data=df_tweet_development)

ax.set_xticklabels(rotation=60, horizontalalignment='right')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Line plot with multiple series
# MAGIC ---
# MAGIC We can draw more than one series in the same plot. We look at an example in the following steps.
# MAGIC 
# MAGIC Find detailed documentation on line plots here:<br><br>
# MAGIC 
# MAGIC - <a href="https://seaborn.pydata.org/tutorial/relational.html#emphasizing-continuity-with-line-plots" target="_blank">Emphasizing continuity with line plots</a>
# MAGIC - <a href="https://seaborn.pydata.org/generated/seaborn.relplot" target="_blank">seaborn.relplot API documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Get the tweets per months for the 3 politicians from above:

# COMMAND ----------

# MAGIC   %sql
# MAGIC   select date_format(created_at, 'yyyy-MM') as `Month`
# MAGIC         ,screen_name as `Twitter User`
# MAGIC         ,count(1) as `Number Tweets`
# MAGIC   from tweets
# MAGIC   where year(created_at) >= 2019
# MAGIC   and screen_name in ('c_lindner', 'SWagenknecht', 'DoroBaer')
# MAGIC   group by `Month`, screen_name 
# MAGIC   order by `Month`

# COMMAND ----------

# MAGIC %md
# MAGIC And as a Pandas data frame:

# COMMAND ----------

df_tweets_per_month = spark.sql('''
  select date_format(created_at, 'yyyy-MM') as `Month`
        ,screen_name as `Twitter User`
        ,count(1) as `Number Tweets`
  from tweets
  where year(created_at) >= 2019
  and screen_name in ('c_lindner', 'SWagenknecht', 'DoroBaer')
  group by `Month`, screen_name 
  order by `Month`
''').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the plot with 3 series by color

# COMMAND ----------

ax = sns.relplot(x="Month"
               ,y="Number Tweets"
               ,kind="line"
               ,height=6
               ,aspect=2
               ,hue="Twitter User"
               ,marker='o'
               ,data=df_tweets_per_month)

ax.set_xticklabels(rotation=45, horizontalalignment='right')
#ax.fig.autofmt_xdate()
