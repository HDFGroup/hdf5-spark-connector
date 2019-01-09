/*
 Run with:

 time spark-shell -i examples/vrtl.scala --jars target/scala-2.11/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar

 **/

import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

sc.setLogLevel("ERROR")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//val flist = "./examples/flist.small.txt"
val flist = "./examples/flist.txt"
//val flist = ""

//val path = "/mnt/wrk/hdftest/GSSTF_NCEP.3/"
val path = ""

val df = { sqlContext.read
  .option("extension", "he5")
  .hdf5(flist, path, "sparky://files")
}
df.count()

val df = { sqlContext.read
  .option("extension", "he5")
  .hdf5(flist, path, "sparky://datasets")
}
df.count()

val df = { sqlContext.read
  .option("extension", "he5")
  .hdf5(flist, path, "sparky://attributes")
}
df.count()

System.exit(0)
