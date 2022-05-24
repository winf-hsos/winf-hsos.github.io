-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Arrays with SQL
-- MAGIC ---
-- MAGIC In this notebook, you'll find examples how to work with arrays in SQL. Arrays are sorted lists.<br>
-- MAGIC 
-- MAGIC **Functions we'll learn in this notebook:**<br><br>
-- MAGIC 
-- MAGIC - `size()` to determine the number of elements in a list (array)
-- MAGIC - The bracket `[]` notation to access specific elements in arrays
-- MAGIC - `transform()` to apply a transformation to all elements in an array
-- MAGIC - `explode()` to transform elements in a list into single rows
-- MAGIC - `posexplode()` to transform elements in a list into single rows along with a column for the index the element had in the original list
-- MAGIC - `array_contains()` to determine if an array contains a specific element
-- MAGIC - `array_distinct()` to remove duplicates from an array
-- MAGIC - `array_except()` to subtract to arrays
-- MAGIC - `array_intersect()` to determine the intersection (overlapping elements) of two arrays
-- MAGIC - `array_union()` to determine the union of two arrays without duplicates
-- MAGIC 
-- MAGIC Other array functions, for which we don't show an example in this notebook, include:
-- MAGIC 
-- MAGIC - `array_join()` to concatenate the elements of an array using a delimiter
-- MAGIC - `array_max()` to get the largest element from an array
-- MAGIC - `array_min()` to get the smallest element from an array
-- MAGIC - `array_position()` to a specific element from an array counting starting with 1
-- MAGIC - `array_remove()` to remove a specific element from an array
-- MAGIC - `array_repeat()` to repeat the elements of an array a specific number of times
-- MAGIC - `array_sort()` to sort an array
-- MAGIC - `arrays_overlap()` to check if two arrays have at least one common element
-- MAGIC - `arrays`
-- MAGIC 
-- MAGIC Refer to this [documentation of all Spark SQL functions](https://docs.databricks.com/spark/latest/spark-sql/language-manual/functions.html) for more information.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load the tweets data

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.sys.process._
-- MAGIC import org.apache.spark.sql.types.{TimestampType}
-- MAGIC import org.apache.spark.sql.types.{IntegerType}
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC spark.conf.set("spark.sql.session.timeZone", "GMT+2")
-- MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
-- MAGIC 
-- MAGIC val table_name = "tweets"
-- MAGIC 
-- MAGIC val file_name = "tweets_ampel.json.gz"
-- MAGIC val url = "https://github.com/winf-hsos/big-data-analytics-code/blob/main/data/tweets_ampel.json.gz?raw=true"
-- MAGIC 
-- MAGIC val localpath = "file:/tmp/" + file_name
-- MAGIC dbutils.fs.rm(localpath)
-- MAGIC 
-- MAGIC "wget " + url + " -O /tmp/" +  file_name !!
-- MAGIC 
-- MAGIC dbutils.fs.rm("dbfs:/datasets/" + file_name)
-- MAGIC dbutils.fs.mkdirs("dbfs:/datasets/")
-- MAGIC dbutils.fs.cp(localpath, "dbfs:/datasets/")
-- MAGIC 
-- MAGIC sqlContext.sql("drop table if exists " + table_name)
-- MAGIC var df = spark.read.option("inferSchema", "true")
-- MAGIC                    .option("quote", "\"")
-- MAGIC                    .option("escape", "\\")
-- MAGIC                    .json("/datasets/" + file_name)
-- MAGIC 
-- MAGIC df = df.withColumn("created_at", unix_timestamp($"created_at", "E MMM dd HH:mm:ss Z yyyy").cast(TimestampType))
-- MAGIC df = df.withColumn("insert_timestamp", unix_timestamp($"insert_timestamp", "yyyy-MM-dd HH:mm:ss.SSS Z").cast(TimestampType))
-- MAGIC df = df.withColumn("favorite_count", $"favorite_count".cast(IntegerType))
-- MAGIC df = df.withColumn("retweet_count", $"retweet_count".cast(IntegerType))
-- MAGIC df.write.saveAsTable(table_name);
-- MAGIC 
-- MAGIC // Clean up
-- MAGIC dbutils.fs.rm("dbfs:/datasets/" + file_name)
-- MAGIC 
-- MAGIC // Show result summary
-- MAGIC display(sqlContext.sql("""
-- MAGIC     select 'tweets', count(1) from tweets
-- MAGIC     """).collect())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Determine the size of an array
-- MAGIC 
-- MAGIC Some columns in our tweets database are of the type `Array`. We can determine the number of elements with the `size()` function. In the example below, we can filter all tweets based on whether they contain at least one hashtag.

-- COMMAND ----------

select hashtags from tweets
where size(hashtags) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Accessing specific elements in an array
-- MAGIC 
-- MAGIC We can directly access specific elements in a list using the bracket notation and passing the index of the element. Note that arrays start counting at zero, so the first element has the index 0.

-- COMMAND ----------

-- Get the first and second hashtag from a tweet
select hashtags[0] as `First Hashtag`
      ,hashtags[1] as `Second Hashtag`
from tweets
where size(hashtags) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE:** The hashtag array above is one-dimensional. An elements in the array has no further elements. Generally, an array's element can in turn be another array, in which case we have a two-dimensional list. To access elements in a two- (or more) dimensional list, we can use two brackets: 
-- MAGIC 
-- MAGIC `two_dim_array[0][0]`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Check if an array contains a specific element
-- MAGIC 
-- MAGIC We can use `array_contains()` to check whether an array contains a specific element.

-- COMMAND ----------

select screen_name as `User Name`
      ,text
from tweets
where array_contains(hashtags, 'covid19')
or array_contains(hashtags, 'Covid19')


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform all elements in an array
-- MAGIC 
-- MAGIC We can use the `transform()` function to transform all elements in an array in the same way. For example, we can make all strings to lower case:

-- COMMAND ----------

select transform(hashtags, h -> lower(h)) as `hashtags_lowercase`
from tweets
where size(hashtags) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Having all strings in lower case makes searching much easier, because we don't need to check both variants:

-- COMMAND ----------

select text
      ,hashtags
from (

  -- This subquery returns all hashtags as lower case
  select text
        ,hashtags
        ,transform(hashtags, h -> lower(h)) as `hashtags_lowercase`
  from tweets
  where size(hashtags) > 0

)
where array_contains(hashtags_lowercase, 'covid19')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform elements of an array into rows
-- MAGIC 
-- MAGIC Sometimes its easier to have each element of an array in a single row. This enables us to apply regular SQL functions such as counting and filtering.

-- COMMAND ----------

select id
      ,hashtags as `Orignal Hashtag Array`
      ,explode(hashtags) as `Hashtag`
from tweets
order by id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If we want to retain the order in which the elements appeared in the array, we can use `posexplode()` to create a second column with that information:

-- COMMAND ----------

select id
      ,hashtags as `Orignal Hashtag Array`
      ,posexplode(hashtags) as (`Index`, `Hashtag`)
from tweets
order by id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once we have each hashtag in a single row, we can count and sort them:

-- COMMAND ----------

select `Hashtag`
      ,count(1) as `Number Occurences`
from (

  -- This subquery transforms the hashtags array into rows
  select id
        ,hashtags as `Orignal Hashtag Array`
        ,explode(hashtags) as `Hashtag`
  from tweets
  order by id
  
)
group by `Hashtag`
order by count(1) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Get only unique elements from an array
-- MAGIC 
-- MAGIC Arrays (or lists) can contain duplicate elements. If we want to reduce an array to unique elements, we can apply `array_distinct()`.

-- COMMAND ----------

select array_distinct(array_with_duplicates) as `array_unique`
from (

  -- This is a dummy subquery to create an array with duplicate elements
  select array('A', 'B', 'C', 'B') as `array_with_duplicates`
  
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Subtract two arrays
-- MAGIC 
-- MAGIC We can subtract an array B from another array A. As a result we get array that contains only elements that are in A, but not in B. For that we can use `array_except()`:

-- COMMAND ----------

select array_except(array_a, array_b) as `subtract_b_from_a`
from (
  
  -- This is a dummy subquery to create two array with overlapping elements
  select array('A', 'B', 'C') as `array_a`
        ,array('B', 'C', 'D') as `array_b`

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Get the intersection of two arrays
-- MAGIC 
-- MAGIC Similar to subtraction, we can determine the overlapping elements of two arrays with `array_intersect()`:

-- COMMAND ----------

select array_intersect(array_a, array_b) as `intersection_a_and_b`
from (
  
  -- This is a dummy subquery to create two array with overlapping elements
  select array('A', 'B', 'C') as `array_a`
        ,array('B', 'C', 'D') as `array_b`

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Get the union of two arrays without duplicates
-- MAGIC 
-- MAGIC To finish the common set operations, we can create the union of two arrays using `array_union()`:

-- COMMAND ----------

select array_union(array_a, array_b) as `union_a_and_b`
from (
  
  -- This is a dummy subquery to create two array with overlapping elements
  select array('A', 'B', 'C') as `array_a`
        ,array('B', 'C', 'D') as `array_b`

)
