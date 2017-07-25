package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ min, max }
import org.apache.spark.sql.types._

class VRTLTests extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("test1.h5").getPath)

  test("Reading sparky://files : single file") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://files")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("fileName", StringType, nullable = false),
        StructField("file size", LongType, nullable = false)))
    assert(df.schema === expectedSchema)

    val value = df.agg(max(df.columns(2))).head
    val expected = Row(106671208.toLong)
    checkRowsEqual(value, expected)
  }

  test("Reading sparky://files : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://files")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("fileName", StringType, nullable = false),
        StructField("file size", LongType, nullable = false)))
    assert(df.schema === expectedSchema)

    val sortedVals = df.drop("fileID").drop("fileName").sort("file size").collect()
    val expected = Array(
      Row(1440L),
      Row(1440L),
      Row(1440L),
      Row(1440L),
      Row(23722L),
      Row(23722L),
      Row(16631632L),
      Row(106671208L))
    assert(sortedVals === expected)
  }
/*
  test("Reading sparky://datasets : single file") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://datasets")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("dataset name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false),
        StructField("number of elements", LongType, nullable = false)))
    assert(df.schema === expectedSchema)

    val sortedVals = df.drop("fileID").drop("dataset name").drop("element type")
      .drop("dimensions").sort("number of elements").collect()
    val expected = Array(Row(0L), Row(1036800L), Row(1036800L), Row(1036800L), Row(1036800L))
    assert(sortedVals === expected)
  }
 */
  test("Reading sparky://datasets : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://datasets")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("dataset name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false),
        StructField("number of elements", LongType, nullable = false)))
    assert(df.schema === expectedSchema)

    val maxValue = df.agg(max(df.columns(4))).head
    val expected = Row(7560000L)
    checkRowsEqual(maxValue, expected)

    val c = df.count()
    assert(c === 56)

    val len = df.drop("fileID").drop("dataset name").drop("element type")
      .drop("number of elements").sort("dimensions").head(2).apply(1).get(0)
    val expect = Array(2, 2, 5)
    assert(len === expect)
  }

  test("Reading sparky://attributes : single file") {
    val df = sqlContext.read.hdf5(h5dir, "sparky://attributes")
    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("object path", StringType, nullable = false),
        StructField("attribute name", StringType, nullable = false),
        StructField("element type", StringType, nullable = false),
        StructField("dimensions", ArrayType(LongType), nullable = false)))
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
        StructField("dimensions", ArrayType(LongType), nullable = false)))
    assert(df.schema === expectedSchema)

    val sortedVals = df.drop("fileID").drop("object path").drop("element type")
      .drop("dimensions").sort("attribute name").head()
    val expected = Row("BeginDate")
    assert(sortedVals === expected)
  }

}
