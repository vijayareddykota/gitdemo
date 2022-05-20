// Databricks notebook source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val logs_DF = spark.read.option("header","true")text("dbfs:/FileStore/shared_uploads/pnaveen1085@gmail.com/Weblog.csv")
val header = logs_DF.first() // Extract Header
val logs_DF1 = logs_DF.filter(row => row != header)
logs_DF1.printSchema()

// COMMAND ----------

logs_DF1.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### a) Parsing the Log Files using RegExp &amp; Pre-process Raw Log Data into Data frame with attributes.

// COMMAND ----------

val hosts = logs_DF1.select (regexp_extract ($"value","""([^(\s|,)]+)""", 1).alias("host"))

hosts.show()

// COMMAND ----------

logs_DF1.select(regexp_extract($"value", """\S(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).alias("Timestamp")).show(false)

// COMMAND ----------

logs_DF1.select(regexp_extract($"value", """\S(\S+)\s(\S+)\s*(\S*)""", 2).alias("url")).show(false)

// COMMAND ----------

logs_DF1.select(regexp_extract($"value", """\S(\w+)\s(\S*)""", 1).alias("Method")).show(false)

// COMMAND ----------

logs_DF1.select(regexp_extract($"value", """(\S+)\s(\S+)\s(\S+)(,)""", 3).alias("HTTP protocol")).show()

// COMMAND ----------

logs_DF1.select(regexp_extract($"value", """,(\d{3})""", 1).cast("int").alias("Status")).show(false)

// COMMAND ----------

val log_df =  logs_DF1.select (regexp_extract ($"value","""([^(\s|,)]+)""", 1).alias("host"),
                 regexp_extract($"value", """\S(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).alias("Timestamp"),
                 regexp_extract($"value", """\S(\S+)\s(\S+)\s*(\S*)""", 2).alias("url"),
                 regexp_extract($"value", """\S(\w+)\s(\S*)""", 1).alias("Method"),
                 regexp_extract($"value", """(\S+)\s(\S+)\s(\S+)(,)""", 3).alias("HTTP protocol"),
                 regexp_extract($"value", """,(\d{3})""", 1).alias("Status"))

 log_df.printSchema()       
log_df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #####  b) Use data cleaning: count null and remove null values. Fix rows with null status (Drop those rows).

// COMMAND ----------

// Find Count of Null, None, NaN of all dataframe columns
import org.apache.spark.sql.functions.{col,when,count}
import org.apache.spark.sql.Column

// UDF
def countNullCols (columns:Array[String]):Array[Column] = {
   columns.map(c => {
   count(when(col(c).isNull, c)).alias(c)
  })
}

log_df.select(countNullCols(log_df.columns): _*).show()



// COMMAND ----------

// MAGIC %md
// MAGIC ##### d) Create new parquet file using cleaned Data Frame. Read the parquet file.

// COMMAND ----------

// Convert Textfile Format to Parquet File Format
// log_df.write.parquet("dbfs:/FileStore/shared_uploads/pnaveen1085@gmail.com/Weblog-2/")


// COMMAND ----------

val parquetLogs = spark.read.parquet("dbfs:/FileStore/shared_uploads/pnaveen1085@gmail.com/Weblog-2")
parquetLogs.show()

// COMMAND ----------

import org.apache.spark.storage.StorageLevel
val parquetLogsDF = parquetLogs.persist(StorageLevel.MEMORY_AND_DISK)


// COMMAND ----------

// MAGIC %md
// MAGIC ##### c) Pre-process and fix timestamp month name to month value. Convert Datetime(timestamp column) as Days, Month &amp; Year.

// COMMAND ----------

val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
                   "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)
// UDF 
def parse_time(s : String):String = {
  "%3$s-%2$s-%1$s %4$s:%5$s:%6$s".format(s.substring(0,2), month_map(s.substring(3,6)), s.substring(7,11), 
                                             s.substring(12,14), s.substring(15,17), s.substring(18))
}

val toTimestamp = udf[String, String](parse_time(_))

val parquetlogsDF = parquetLogs.select($"*", to_timestamp(toTimestamp($"Timestamp")).alias("time")).drop("Timestamp")
parquetlogsDF.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### e) Show the summary of each column.

// COMMAND ----------

   parquetlogsDF.describe(cols = "Status").show()            

// COMMAND ----------

val daily_hosts = parquetlogsDF.withColumn("day",dayofyear($"time")).withColumn("month",month($"time")).withColumn("year",year($"time"))                   
daily_hosts.show(5)


// COMMAND ----------

// MAGIC %md
// MAGIC ##### f) Display frequency of 200 status code in the response for each month.

// COMMAND ----------

daily_hosts.filter($"Status" === 200).groupBy("month").count().sort(desc("count")).show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC ##### g) Frequency of Host Visits in November Month.

// COMMAND ----------

spark.sql("select host,month, count(*) as Count from weblogsTableone where Status = 404 group by host,month").show()


// COMMAND ----------

daily_hosts.createOrReplaceTempView("weblogsTableone")

// COMMAND ----------

spark.sql("select * from weblogsTableone limit 5").show()

// COMMAND ----------

// spark.sql("select year,month, count(host) as Count from weblogsTableone where month = 11 and Status = 200 group by year,month order by year").show()
spark.sql("select host,month, count(*) as Count from weblogsTableone where Status = 404 group by host,month").show()


// COMMAND ----------

// MAGIC %md
// MAGIC ##### h) Display Top 15 Error Paths - status != 200.

// COMMAND ----------

spark.sql("select url, count(*) as Count from weblogsTableone where Status != 200 group by url order by Count Desc limit 15").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### i) Display Top 10 Paths with Error - with status equals 200.

// COMMAND ----------

// i
spark.sql("select url, count(*) as Count from weblogsTableone where Status = 200 group by url order by Count Desc limit 15").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### j) Exploring 404 status code. Listing 404 status Code Records. List Top 20 Host with 404 response status code (Query + Visualization).

// COMMAND ----------

// J
spark.sql("select host, count(*) as Count from weblogsTableone where Status = 404  group by host order by Count Desc limit 20").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select host, count(*) as Count from weblogsTableone where Status = 404  group by host order by Count Desc limit 20

// COMMAND ----------

// MAGIC %md
// MAGIC ##### k) Display the List of 404 Error Response Status Code per Day (Query + Visualization).

// COMMAND ----------

spark.sql("select day, count(*) as Count from weblogsTableone where Status = 404  group by day order by day").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select day, count(*) as Count from weblogsTableone where Status = 404  group by day order by Count Desc limit 20

// COMMAND ----------

// MAGIC %md
// MAGIC ##### l) List Top 20 Paths (Endpoint) with 404 Response Status Code.

// COMMAND ----------

// L
spark.sql("select url, count(*) as Count from weblogsTableone where Status = 404 group by url order by Count Desc limit 20").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### m) Query to Display Distinct Path responding 404 in status error.

// COMMAND ----------

// m
spark.sql("select distinct(url) from weblogsTableone where Status = 404").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### n) Find the number of unique source IPs that have made requests to the webserver for each month.

// COMMAND ----------

spark.sql("select host,url,month, count(*) from weblogsTableone where status = 200 group by host,url,month limit 10").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### o) Display the top 20 requested Paths in each Month (Query + Visualization).

// COMMAND ----------

spark.sql("select month,url, count(url) as Count from weblogsTableone where Status = 200 group by month,url order by Count Desc limit 20").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select month,url, count(url) as Count from weblogsTableone where Status = 200 group by month,url order by Count Desc limit 20

// COMMAND ----------


