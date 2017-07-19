// INSTRUCTIONS TO RUN:
// time spark-shell -i examples/blog.scala --jars target/scala-2.11/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar --packages com.databricks:spark-csv_2.10:1.4.0
// cat file.csv/*.csv > output.csv

import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val dirName = "/mnt/wrk/hdftest/GSSTF_NCEP.3/2000"
val pathName = "/Users/Alan/IdeaProjects/sparky/examples/GSSTF_NCEP_mini"
val varName = "/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m"

val df = sqlContext.read.
  option("extension", "he5").
  option("window size", "200000").
  hdf5(pathName, varName)

// extract the date from the file name
val getDate = udf((file: String) =>
  file.substring(file.lastIndexOf("GSSTF_NCEP.3") + 13,
    file.lastIndexOf(".he5")))

val nonFill = df.where($"value" > -999.0)

// Top 10 and Bottom 10
val w = Window.partitionBy($"fileID").orderBy($"value".desc)
val dfTop = nonFill.withColumn("rn", row_number.over(w)).where($"rn" <= 10).drop("rn")
val w2 = Window.partitionBy($"fileID").orderBy($"value".asc)
val dfTop2 = nonFill.withColumn("rn", row_number.over(w2)).where($"rn" <= 10).drop("rn")
val dfTop3 = dfTop.unionAll(dfTop2)
val values = collect_list($"value").alias("values")
dfTop3.groupBy($"fileID").agg(values).show(false)

// Overall Summary
nonFill.registerTempTable("df1")
val df2 = sqlContext.sql(
  """SELECT fileID,
            count(value) AS cnt,
            min(value) AS min,
            avg(value) AS mean,
            percentile_approx(value, 0.5) AS median,
            max(value) AS max,
            stddev(value) AS stddev
     FROM df1 GROUP BY fileID""")
df2.show(20)

val idMap = sqlContext.read.
  option("extension", "he5").hdf5(pathName, "sparky://files")

val dateMap = idMap.drop("file size").
  withColumn("fileName", getDate($"fileName"))

val meanEtMedian = df2.
  join(dateMap, "fileID").
  drop("fileID").
  drop("cnt").
  drop("min").
  drop("max").
  drop("stddev").
  sort("fileName").
  select("fileName", "mean", "median")

meanEtMedian.show(20)

meanEtMedian.write.
  format("com.databricks.spark.csv").
  option("header", "false").
  save("file.csv")

System.exit(0)
