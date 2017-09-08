// Copyright (C) 2017 The HDF Group
//  All Rights Reserved 
//
// HOW TO RUN:
//
// $spark-2.3.0-SNAPSHOT-bin-hdf5s-0.0.1/bin/spark-shell -i demo.scala

import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("Spark SQL HDF5 example").getOrCreate()

// We assume that HDF5 files (e.g., GSSTF_NCEP.3.2008.12.31.he5) are 
// under /tmp directory. Change the path name ('/tmp') if necessary.
val df=spark.read.option("extension", "he5").option("recursion", "false").hdf5("/tmp/", "/HDFEOS/GRIDS/NCEP/Data Fields/SST")

// Let's print some values from the dataset.
df.show()

// The output will look like below.
//
//+------+-----+------+
//|FileID|Index| Value|
//+------+-----+------+
//|     0|    0|-999.0|
//|     0|    1|-999.0|
//|     0|    2|-999.0|
//...

System.exit(0)
