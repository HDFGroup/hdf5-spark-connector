// Copyright (C) 2017 The HDF Group
// All rights reserved

// How to run on nene:
//
// $time /Users/hyoklee/src/spark-2.3.0-SNAPSHOT-bin-hdf5s-0.0.1/bin/spark-shell -i orc_read.scala --driver-memory 1G --executor-memory 1G --master spark://nene.ad.hdfgroup.org:7077

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df = spark.read.orc("/tmp/orc/test_table")

val nonFill = df.where($"value" > -999.0)

// Top 10 and Bottom 10
val w = Window.partitionBy($"fileID").orderBy($"value".desc)
val dfTop = nonFill.withColumn("rn", row_number.over(w)).where($"rn" <= 10).drop("rn")
val w2 = Window.partitionBy($"fileID").orderBy($"value".asc)
val dfTop2 = nonFill.withColumn("rn", row_number.over(w2)).where($"rn" <= 10).drop("rn")
val dfTop3 = dfTop.unionAll(dfTop2)
val values = collect_list($"value").alias("values")
dfTop3.groupBy($"fileID").agg(values).show(false)

// Overall summary
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
System.exit(0)
