// Databricks notebook source
// MAGIC %md
// MAGIC # Clean-Up Databricks Files and Tables
// MAGIC ---
// MAGIC The maximum quota for the Databricks Community Edition is either 10.000 files or 10 GB of storage. When exceeded, we cannot perform analysis anymore. It's time to clean up!

// COMMAND ----------

// MAGIC %md
// MAGIC ## Clean-Up the temporary data set folder
// MAGIC ---
// MAGIC The import scripts we use store the source file in a folder named `/datasets`. The following code deletes all files from that folder.

// COMMAND ----------

val PATH = "dbfs:/datasets/"
dbutils.fs.ls(PATH)
            .map(_.name)
            .foreach((file: String) => dbutils.fs.rm(PATH + file, true))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Clean-Up `tmp` folder
// MAGIC ---
// MAGIC When downloading a file from the internet, as the import script frequently does, there is a local copy of that file stored in a folder called `/tmp`. The following code deletes all files from that folder, too:

// COMMAND ----------

val PATH = "/tmp/"
dbutils.fs.ls(PATH)
            .map(_.name)
            .foreach((file: String) => dbutils.fs.rm(PATH + file, true))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Clean-Up `FileStore/viz` folder
// MAGIC ---
// MAGIC When plotting we store exports of visualizations in the folder `FileStore/viz`. It is a good idea to clean up here too:

// COMMAND ----------

val PATH = "/FileStore/viz"
dbutils.fs.ls(PATH)
            .map(_.name)
            .foreach((file: String) => dbutils.fs.rm(PATH + file, true))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Delete tables and view
// MAGIC ---
// MAGIC To free even more space, you can delete tables that you no longer need:

// COMMAND ----------

// MAGIC %sql
// MAGIC -- This command shows a list of all tables and views
// MAGIC show tables

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Replace with one of your own tables that you no longer need
// MAGIC drop table my_table
// MAGIC 
// MAGIC -- It can also be a view, in which case you need to run this
// MAGIC -- drop view my_view

// COMMAND ----------

// MAGIC %md
// MAGIC ## List folders
// MAGIC ---
// MAGIC To check whether a folder has been deleted (or its content), you can use the `dbutils.fs.ls()` command:

// COMMAND ----------

display(dbutils.fs.ls("/FileStore"))
