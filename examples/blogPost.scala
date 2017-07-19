// time spark-shell -i examples/blogPost.scala --jars target/distribution/scala-2.11/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar --packages com.databricks:spark-csv_2.10:1.4.0
// cat *.csv > output.csv

import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val pathName = "/Users/Alan/IdeaProjects/sparky/examples/GSSTF_NCEP_mini"

//
// CHANGE THE PATH TO DIRECTORY ACCORDINGLY
//
val df = sqlContext.read.option("extension", "he5").option("window size", "20000").hdf5(pathName, "/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m")

// get date from file name
val getDate = udf((file: String) => file.substring(70,80))

// non fill-values
val filtered = df.where($"value" > -999.0)

val median = filtered.stat.approxQuantile("value", Array(0.5), 0)

// SUMMARIZE:
filtered.select(count("value"), avg("value"), stddev("value"), min("value"), max("value")).withColumn("median(value)", lit(median(0))
  ).select("count(value)", "avg(value)", "median(value)",
    "stddev_samp(value)", "min(value)", "max(value)").show

/* determine the number of distinct values
 val distinctFiltered = filtered.select(filtered("value")).distinct
 distinctFiltered.select(count("value")).show */

filtered.registerTempTable("df1")
val df2 = sqlContext.sql("select fileID, percentile_approx(value, 0.5) as approxQuantile from df1 group by fileID")

// MEAN AND MEDIAN OVER TIME GRAPH:
//
// CHANGE THE PATH TO DIRECTORY ACCORDINGLY
//
val idMap = sqlContext.read.option("extension", "he5").hdf5(pathName, "sparky://files")
val dateMap = idMap.drop("file size").withColumn("fileName", getDate($"fileName"))

// median and mean with dates
val dataframe = filtered.groupBy("fileID").agg(avg("value")).join(df2, "fileID").
  join(dateMap, "fileID").drop("fileID").sort("fileName").select("fileName", "avg(value)", "approxQuantile")
//val dataframe = filtered.groupBy("fileID").agg(avg("value")).withColumn("median(value)", median).join(dateMap,
  //"fileID").drop("fileID").sort("fileName").select("fileName", "avg(value)", "median(value)")

dataframe.show

// dataframe.write.format("com.databricks.spark.csv").option("header", "false").save("file.csv")

// TOP AND BOTTOM 10:

System.exit(0)
