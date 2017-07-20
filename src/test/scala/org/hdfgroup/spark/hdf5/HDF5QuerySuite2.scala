package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ min, max }
import org.apache.spark.sql.types._

class HDF5QuerySuite2 extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("test1.h5").getPath
  )

  val stm3file = getClass.getResource("LAAA.h5").toString
  val stm3dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("LAAA.h5").getPath
  )

  val bidtest = "/LAAA/Quotes/Bid"
  val qtrtest = "/LAAA/Quarter/QuotesStartBlock"
  val montest = "/LAAA/Month/QuotesStartBlock"
  val wktest = "/LAAA/Week/QuotesStartBlock"
  val qtroffset = 5670000
  val monoffset = 6930000
  val wkoffset = 7440000

  test("Get the year high bid") {
    val biddf = sqlContext.read.hdf5(stm3file, bidtest)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
    ))
    assert(biddf.schema === expectedSchema)

    val bid = biddf.agg(max(biddf.columns(2))).head
    val expected = Row(19.999998f)
    checkRowsEqual(bid, expected)
  }

  test("Get the last quarter high bid") {
    val biddf = sqlContext.read.hdf5(stm3file, bidtest)
    val qtrdf = biddf.where(biddf("index0") >= qtroffset)
    val bid = qtrdf.agg(max(qtrdf.columns(2))).head
    val expected = Row(19.99998f)
    checkRowsEqual(bid, expected)
  }

  test("Get the last month's high bid") {
    val biddf = sqlContext.read.hdf5(stm3file, bidtest)
    val mondf = biddf.where(biddf("index0") >= monoffset)
    val bid = mondf.agg(max(mondf.columns(2))).head
    val expected = Row(19.999973f)
    checkRowsEqual(bid, expected)
  }

  test("Get the last week's high bid") {
    val biddf = sqlContext.read.hdf5(stm3file, bidtest)
    val wkdf = biddf.where(biddf("index0") >= wkoffset)
    val bid = wkdf.agg(max(wkdf.columns(2))).head
    val expected = Row(19.999973f)
    checkRowsEqual(bid, expected)
  }

  val twodimfile = getClass.getResource("test1.h5").toString
  val twodimtest = "/dimensionality/2dim"

  test("Read a linearized 2D array") {
    val df = sqlContext.read.hdf5(twodimfile, twodimtest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", IntegerType, nullable = false)
    ))
    assert(df.schema === expectedSchema)

    val expected = (0 until 30).map { x => Row(x, x.toInt) }
    checkEqual(df.drop("fileID"), expected)
  }

  val gfile = getClass.getResource("GSSTF_NCEP.h5").toString
  val ssttest = "/HDFEOS/GRIDS/NCEP/Data Fields/SST"

  test("Reading linearized 2D array : check row count") {
    val df = sqlContext.read.hdf5(gfile, ssttest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
    ))
    assert(df.schema === expectedSchema)

    val count = Row(df.count())
    val expected = Row(720 * 1440.toLong)
    checkRowsEqual(count, expected)
  }

  test("Reading linearized 2D array : check distinct row index count") {
    val df = sqlContext.read.hdf5(gfile, ssttest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
      ))
    assert(df.schema === expectedSchema)

    val distinctCount = Row(df.select(df("index0")).distinct().count())
    val expected = Row(720 * 1440.toLong)
    checkRowsEqual(distinctCount, expected)
  }

  test("Reading linearized 2D array : check minimum row index") {
    val df = sqlContext.read.hdf5(gfile, ssttest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
      ))
    assert(df.schema === expectedSchema)

    val minimumIdx = df.agg(min(df.columns(1))).head
    val expected = Row(0.toLong)
    checkRowsEqual(minimumIdx, expected)
  }

  test("Reading linearized 2D array : check maximum row index") {
    val df = sqlContext.read.hdf5(gfile, ssttest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
      ))

    val minimumIdx = df.agg(max(df.columns(1))).head
    val expected = Row((720 * 1440 - 1).toLong)
    checkRowsEqual(minimumIdx, expected)
  }

  test("Reading linearized 2D array : check minimum value") {
    val df = sqlContext.read.hdf5(gfile, ssttest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
      ))
    assert(df.schema === expectedSchema)

    val minVal = df.agg(min(df.columns(2))).head
    val expected = Row(-999.0f)
    checkRowsEqual(minVal, expected)
  }

  test("Reading sparky://files : single file") {
    val df = sqlContext.read.hdf5(gfile, "sparky://files")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("fileName", StringType, nullable = false),
        StructField("file size", LongType, nullable = false)
      ))
    assert(df.schema === expectedSchema)

    val value = df.agg(max(df.columns(2))).head
    val expected = Row(16631632.toLong)
    checkRowsEqual(value, expected)
  }

  test("Reading sparky://files : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://files")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("fileName", StringType, nullable = false),
        StructField("file size", LongType, nullable = false)
      ))
    assert(df.schema === expectedSchema)

    val sortedVals = df.drop("fileID").drop("fileName").sort("file size").collect()
    val expected = Array(Row(1440L), Row(1440L), Row(23722L), Row(16631632L), Row(106671208L))
    assert(sortedVals === expected)
  }

  test("Reading sparky://datasets : single file") {
    val df = sqlContext.read.hdf5(gfile, "sparky://datasets")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("dataset name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false),
        StructField("number of elements", LongType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val sortedVals = df.drop("fileID").drop("dataset name").drop("element type")
      .drop("dimensions").sort("number of elements").collect()
    val expected = Array(Row(0L), Row(1036800L), Row(1036800L), Row(1036800L), Row(1036800L))
    assert(sortedVals === expected)
  }

  test("Reading sparky://datasets : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://datasets")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("dataset name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false),
        StructField("number of elements", LongType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val maxValue = df.agg(max(df.columns(4))).head
    val expected = Row(7560000L)
    checkRowsEqual(maxValue, expected)

    val c = df.count()
    assert(c === 40)

    val len = df.drop("fileID").drop("dataset name").drop("element type")
      .drop("number of elements").sort("dimensions").head(2).apply(1).get(0)
    val expect = Array(2, 2, 5)
    assert(len === expect)
  }

  test("Reading sparky://attributes : single file") {
    val df = sqlContext.read.hdf5(gfile, "sparky://attributes")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("object path", StringType, nullable = false),
        StructField("attribute name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val sortedVals = df.drop("fileID").drop("object path").drop("attribute name")
      .drop("dimensions").sort("element type").head()
    val expected = Row("FLString")
    assert(sortedVals === expected)
  }

  test("Reading sparky://attributes : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://attributes")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("object path", StringType, nullable = false),
        StructField("attribute name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    // df.show(false)

    val sortedVals = df.drop("fileID").drop("object path").drop("element type")
      .drop("dimensions").sort("attribute name").head()
    val expected = Row("BeginDate")
    assert(sortedVals === expected)
  }

}
