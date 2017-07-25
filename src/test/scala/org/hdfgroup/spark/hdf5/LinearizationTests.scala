package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ min, max }
import org.apache.spark.sql.types._

class LinearizationTests extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("test1.h5").getPath)

  val twodimfile = getClass.getResource("test1.h5").toString
  val twodimtest = "/dimensionality/2dim"

  test("Reading linearized 2D array") {
    val df = sqlContext.read.hdf5(twodimfile, twodimtest)
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", IntegerType, nullable = false)))
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
        StructField("value", FloatType, nullable = false)))
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
        StructField("value", FloatType, nullable = false)))
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
        StructField("value", FloatType, nullable = false)))
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
        StructField("value", FloatType, nullable = false)))

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
        StructField("value", FloatType, nullable = false)))
    assert(df.schema === expectedSchema)

    val minVal = df.agg(min(df.columns(2))).head
    val expected = Row(-999.0f)
    checkRowsEqual(minVal, expected)
  }

}
