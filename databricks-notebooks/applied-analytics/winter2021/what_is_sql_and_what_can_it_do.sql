-- Databricks notebook source
-- MAGIC %md
-- MAGIC # What is SQL and what can it do?
-- MAGIC ---
-- MAGIC This notebook is published along with the article ["What is SQL and what can it do" on Medium](https://medium.com/@nicolas.meseth/what-is-sql-and-what-can-it-do-a2ad0204e47).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparation
-- MAGIC ---
-- MAGIC Before we can query the data with SQL, we must do some preparation work:<br><br>
-- MAGIC 
-- MAGIC 1. Load the data
-- MAGIC 2. Get an overview of the columns in the data set

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Load the data set into a table in your Databricks account
-- MAGIC ---
-- MAGIC The following Python code fetches the `.csv`-file with the data from my public <a href="https://github.com/winf-hsos/datalit" target="_blank">Github repository</a>. It then reads the file and creates a table named `sex_offenders` from it, so that we can access the data with SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import requests
-- MAGIC from pyspark.sql.functions import to_timestamp
-- MAGIC 
-- MAGIC # Choose a name for your table in Databricks
-- MAGIC table_name = "sex_offenders"
-- MAGIC 
-- MAGIC # URL to Sex Offenders dataset on Github
-- MAGIC url = "https://raw.githubusercontent.com/winf-hsos/datalit/master/sql/introduction/what_is_sql_and_what_can_it_do/Sex_Offenders.csv"
-- MAGIC 
-- MAGIC localpath = "/tmp/" + table_name + ".csv"
-- MAGIC dbutils.fs.rm("file:" + localpath)
-- MAGIC 
-- MAGIC # Get the file from the URL
-- MAGIC r = requests.get(url, allow_redirects=True)
-- MAGIC open(localpath, 'wb').write(r.content)
-- MAGIC 
-- MAGIC dbutils.fs.mkdirs("dbfs:/datasets")
-- MAGIC dbutils.fs.cp("file:" + localpath, "dbfs:/datasets")
-- MAGIC 
-- MAGIC spark.sql("drop table if exists " + table_name)
-- MAGIC df = spark.read.option("header", "true").option("inferSchema", "true").csv("/datasets/" + table_name + ".csv");
-- MAGIC 
-- MAGIC # Rename columns
-- MAGIC df = df.withColumnRenamed("LAST", "last_name")
-- MAGIC df = df.withColumnRenamed("FIRST", "first_name")
-- MAGIC df = df.withColumnRenamed("BLOCK", "block")
-- MAGIC df = df.withColumnRenamed("GENDER", "gender")
-- MAGIC df = df.withColumnRenamed("RACE", "race")
-- MAGIC df = df.withColumnRenamed("HEIGHT", "height")
-- MAGIC df = df.withColumnRenamed("VICTIM MINOR", "victim_minor")
-- MAGIC df = df.withColumnRenamed("BIRTH DATE", "birth_date")
-- MAGIC 
-- MAGIC # Drop unnecessary columns
-- MAGIC df = df.drop("AGE")
-- MAGIC 
-- MAGIC # Convert columns data types
-- MAGIC df = df.withColumn("birth_date", to_timestamp("birth_date", "MM/dd/yyyy"))
-- MAGIC df.write.saveAsTable(table_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Get an overview of the columns in the data set
-- MAGIC ---
-- MAGIC When dealing with a new data set, it is useful to know which columns it contains. In SQL, the `describe`-command outputs a table with the column names and data types.

-- COMMAND ----------

describe sex_offenders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL can select columns
-- MAGIC 
-- MAGIC While a table can have any number of columns, the SELECT-command allows to select a subset of the existing columns. In most cases, only a few columns are needed to answer a question. The selection of a subset is also called **projection** in terms of the [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra), which forms a theoretical foundation for most operations we perform with SQL.
-- MAGIC 
-- MAGIC Imagine we have a table `sex_offenders` with 7 columns that contain different information about a person in the dataset (e.g. name, date of birth, or gender). Suppose we are interested in querying only the first and last name of a person. Assuming we know the structure of the table and accordingly the column names, we can use SQL to query the subset with the columns of interest **by specifying the names of the columns separated by commas** after the SELECT-keyword:

-- COMMAND ----------

SELECT
  first_name,
  last_name
FROM
  sex_offenders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Aside from selecting existing columns, we can derive a new column using an arbitrary expression, assign a name to it, and return it in the result of the query. Assume we want to calculate the age of a person based on their date of birth. We can create a column `Age` that contains this information using a simple calculation:

-- COMMAND ----------

SELECT
  YEAR(CURRENT_TIMESTAMP()) - YEAR(birth_date) AS `Age`
FROM
  sex_offenders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the calculation above, we use the built-in function `YEAR()` to extract the year from the current date and the birth date of every person in the dataset and substract both.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL can filter rows
-- MAGIC 
-- MAGIC In addition to selecting columns, we can use SQL to reduce the result to exactly the rows we need to answer a question. A table often contains many thousands, millions or more entries. Often only a few of them are relevant for answering a specific question.
-- MAGIC 
-- MAGIC Imagine we only want to select the female persons in our dataset. Using the column `gender` we can formulate a corresponding ***condition*** and apply it to the result of our query with the WHERE-clause:

-- COMMAND ----------

SELECT 
   first_name,
   last_name,
   gender
FROM sex_offenders
WHERE gender = "FEMALE"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL can sort results
-- MAGIC ---
-- MAGIC An important function for any data analysis tool is the sorting of results. Whether we want to sort a list of persons alphabetically by name or determine the 100 oldest persons: Sorting the data by one or more columns is the way to do it.
-- MAGIC 
-- MAGIC The following query gives us a list of persons sorted in ascending order by last name:

-- COMMAND ----------

SELECT
  first_name,
  last_name
FROM
  sex_offenders
ORDER BY
  last_name ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL can aggregate and group data
-- MAGIC ---
-- MAGIC The last important function of the SELECT command is the ability to aggregate many records (rows) to only a few. There are 2 possibilities:<br><br>
-- MAGIC 
-- MAGIC 1. Simple aggregation over all records (after applying the filter conditions).
-- MAGIC 2. Aggregation over subsets and formation of groups based on one or more characteristics (columns)
-- MAGIC 
-- MAGIC An example of the first option is counting the rows of a table. The following example counts how many customers are contained in the table `sex_offenders`:

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  sex_offenders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC An example of the second option is to determine the number of persons for each gender. We can determine this with the `GROUP BY` command as follows:

-- COMMAND ----------

SELECT
  gender,
  count(*)
FROM
  sex_offenders
GROUP BY
  gender

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC ---
-- MAGIC We have seen how we can use SQL and the SELECT command to perform simple queries on structured data. We introduced the 4 basic functions of data queries with SQL:<br><br>
-- MAGIC 
-- MAGIC 1. Select columns
-- MAGIC 2. Filter rows
-- MAGIC 3. Sort data
-- MAGIC 4. Aggregate and group data
