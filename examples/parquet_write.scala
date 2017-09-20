// Copyright (C) 2017 The HDF Group
// All rights reserved

// How to run on nene:
//
// $time /Users/hyoklee/src/spark-2.3.0-SNAPSHOT-bin-hdf5s-0.0.1/bin/spark-shell -i parquet_write.scala --driver-memory 1G --executor-memory 1G --master spark://nene.ad.hdfgroup.org:7077

import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val directory = "/tmp/sparky"
val varName = "/HDFEOS/GRIDS/NCEP/Data Fields/SST"
val df = sqlContext.read.
  option("extension", "he5").
  option("window size", "20000").
  hdf5(directory, varName)
df.write.parquet("/tmp/parquet/test_table")
System.exit(0)
