/*
 Run with:

 time spark-shell -i examples/vrtl.scala --jars target/scala-2.11/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar

 **/

import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val files = "/mnt/wrk/hdftest/GSSTF_NCEP.3/"

val df = sqlContext.read.option("extension", "he5").hdf5(files, "sparky://files")
df.count()

val df = sqlContext.read.option("extension", "he5").hdf5(files, "sparky://datasets")
df.count()

val df = sqlContext.read.option("extension", "he5").hdf5(files, "sparky://attributes")
df.count()

/*
val df = sqlContext.read.hdf5("//GSSTF_NCEP.h5","/HDFEOS/GRIDS/NCEP/Data Fields/SST")

df.filter($"value" > -999).describe("value").show()

println(df.filter($"value" > -999).sort("value").collect().drop(df.filter($"value" > -999).sort("value").collect().length/2).head)
*/

System.exit(0)
