import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val files = "data/sub"

println("sparky://files")
val df = sqlContext.read.hdf5(files, "sparky://files")
df.show(false)

println("sparky://datasets")
val df = sqlContext.read.hdf5(files, "sparky://datasets")
df.show(false)

println("sparky://attributes")
val df = sqlContext.read.hdf5(files, "sparky://attributes")
df.show(false)

System.exit(0)
