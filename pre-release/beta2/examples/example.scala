import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

sc.setLogLevel("ERROR")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val files = "data"
val dataset = "/multi"
val test1 = "data/test1.h5"
val twodim = "/dimensionality/2dim"

println("With recursion:")
val df = sqlContext.read.hdf5(files, dataset)
df.show()

println("Without recursion:")
val df = sqlContext.read.option("recursion", "false").hdf5(files, dataset)
df.show()

println("Full dataset:")
val df = sqlContext.read.hdf5(test1, twodim)
df.show()

println("Hyperslab:")
val df = sqlContext.read. option("start", "1,1").option("block", "2,2").hdf5(test1, twodim)
df.show()

System.exit(0)
