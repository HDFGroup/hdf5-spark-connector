import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

sc.setLogLevel("ERROR")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val baseDir = "data"
val subDir = "data/sub"

// Show all HDF5 files below PATH. The default extension is '.h5'.
// Use an option to provide other extensions.

println("===>> data/**.[h5,hdf5] @ sparky://files\n")
val df = {
  sqlContext.read
    .option("extension","h5,hdf5")
    .hdf5(baseDir, "sparky://files")
}
df.show(false)

// Show information about HDF5 datasets in files in the top level directory
// only.

println("===>> data/*.h5 @ sparky://datasets\n")
val df = {
  sqlContext.read
    .option("recursion", false)
    .hdf5(baseDir, "sparky://datasets")
}
df.show(false)

// Show information about HDF5 attributes in files in the subdirectory.

println("===>> data/sub/*.hdf5 @ sparky://attributes\n")
val df = {
  sqlContext.read
    .option("extension","hdf5")
    .hdf5(subDir, "sparky://attributes")
}
df.show(false)

// Read a dataset that's present in all files

println("===>> data/**.[h5,hdf5] @ /multi\n")
val df = {
  sqlContext.read
    .option("extension","h5,hdf5")
    .hdf5(baseDir, "/multi")
}
df.show(100, false)

// Read a 2D dataset that's present in only two files

println("===>> data/test1.h5,data/test1.hdf5 @ /dimensionality/2dim\n")
val df = {
  sqlContext.read
    .option("extension","h5,hdf5")
    .hdf5(baseDir, "/dimensionality/2dim")
}
df.show(100, false)

// Read only a region (hyperslab) of that dataset

println("===>> data/test1.h5,data/test1.hdf5 @ /dimensionality/2dim[1:3,1:3]\n")
val df = {
  sqlContext.read
    .option("extension","h5,hdf5")
    .option("start", "1,1").option("block", "2,2")
    .hdf5(baseDir, "/dimensionality/2dim")
}
df.show(100, false)



System.exit(0)
