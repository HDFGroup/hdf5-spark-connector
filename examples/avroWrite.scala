// time spark-shell -i examples/avroWrite.scala --jars target/scala-2.11/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar --packages com.databricks:spark-avro_2.11:3.2.0

import com.databricks.spark.avro._
import org.hdfgroup.spark.hdf5._

sc.setLogLevel("ERROR")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val flist = "examples/flist.2000.txt"
val dirName = "/mnt/wrk/hdftest/GSSTF_NCEP.3/2000"
val varName = "/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m"

val df = { sqlContext.read
  .option("extension", "he5")
  .option("window size", "200000")
  .hdf5(flist, "", varName)
}

df.write.avro("df.avro")

System.exit(0)
