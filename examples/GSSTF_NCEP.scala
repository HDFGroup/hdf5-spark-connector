import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val directory = "/mnt/wrk/hdftest/GSSTF_NCEP.3"
val variable = "/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m"

val df = sqlContext.read.
  option("extension", "he5").
  option("window size", "550000").
  hdf5(directory, variable)

// non fill-values

val nonFillDF = df.where($"value" > -999.0)

nonFillDF.
   select(count("value"),
            min("value"),
            avg("value"),
            max("value"),
            stddev("value")).show

// determine the number of distinct values

val distinctNonFillDF = nonFillDF.select(nonFillDF("value")).distinct

distinctNonFillDF.
   select(count("value"),
            min("value"),
            avg("value"),
            max("value"),
            stddev("value")).show

// group by file ID

nonFillDF.
   groupBy("fileID").
   agg(count("value"),
   min("value"),
   avg("value"),
   max("value"),
   stddev("value")).show(20)

System.exit(0)
