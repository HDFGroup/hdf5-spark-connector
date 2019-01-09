// time spark-shell -i examples/5parkyCount.scala --jars target/scala-2.11/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar

import org.hdfgroup.spark.hdf5._

sc.setLogLevel("ERROR")

val flist = "examples/flist.2000.txt"
val varName = "/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m"

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = { sqlContext.read
  .option("extension", "he5")
  .option("window size", "200000")
  .hdf5(flist, "", varName)
}
df.count()

System.exit(0)
