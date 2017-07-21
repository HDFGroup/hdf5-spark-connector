package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ min, max }
import org.apache.spark.sql.types._

class HDF5QuerySuite extends FunTestSuite {

  val h5file = getClass.getResource("test1.h5").toString
  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("test1.h5").getPath)

  val multiDataset = "/multi"
  val int8test = "/datatypes/int8"
  val int16test = "/datatypes/int16"
  val int32test = "/datatypes/int32"
  val int64test = "/datatypes/int64"
  val uint8test = "/datatypes/uint8"
  val uint16test = "/datatypes/uint16"
  val uint32test = "/datatypes/uint32"
  val float32test = "/datatypes/float32"
  val float64test = "/datatypes/float64"

  test("Reading multiple files") {
    val df = sqlContext.read.hdf5(h5dir, multiDataset).drop("fileID")
    val expected = (0 until 30).map { x => Row(x % 10, x) }

    checkEqual(df, expected)
  }

  test("Read files in chunks") {
    val evenchunkeddf = sqlContext.read
      .option("window size", 5.toString)
      .hdf5(h5file, int8test).drop("fileID")

    val oddchunkeddf = sqlContext.read
      .option("window size", 3.toString)
      .hdf5(h5file, int8test).drop("fileID")

    val expected = Row(0L, Byte.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toByte) } :+
      Row(9L, Byte.MaxValue)

    checkEqual(evenchunkeddf, expected)
    checkEqual(oddchunkeddf, expected)
  }

  // Signed integer tests

  test("Reading int8") {
    val df = sqlContext.read.hdf5(h5file, int8test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", ByteType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Byte.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toByte) } :+
      Row(9L, Byte.MaxValue)

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading int16") {
    val df = sqlContext.read.hdf5(h5file, int16test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", ShortType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Short.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toShort) } :+
      Row(9L, Short.MaxValue)

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading int32") {
    val df = sqlContext.read.hdf5(h5file, int32test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", IntegerType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Int.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toInt) } :+
      Row(9L, Int.MaxValue)

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading int64") {
    val df = sqlContext.read.hdf5(h5file, int64test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", LongType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Long.MinValue) +:
      (1L until 9L).map { x => Row(x, x - 2) } :+
      Row(9L, Long.MaxValue)

    checkEqual(df.drop("fileID"), expected)
  }

  // Unsigned integer tests

  test("Reading uint8") {
    val df = sqlContext.read.hdf5(h5file, uint8test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", ShortType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = (0L until 9L).map { x => Row(x, x.toShort) } :+ Row(9L, 255)

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading uint16") {
    val df = sqlContext.read.hdf5(h5file, uint16test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", IntegerType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = (0L until 9L).map { x => Row(x, x.toInt) } :+ Row(9L, 65535)

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading uint32") {
    val df = sqlContext.read.hdf5(h5file, uint32test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", LongType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = (0L until 9L).map { x => Row(x, x) } :+ Row(9L, 4294967295L)

    checkEqual(df.drop("fileID"), expected)
  }

  // Float tests

  test("Reading float32") {
    val df = sqlContext.read.hdf5(h5file, float32test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", FloatType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = (0 until 10).map(x => x % 2 match {
      case 0 => Row(x, (0.2 * x).toFloat)
      case 1 => Row(x, (-0.2 * x).toFloat)
    })

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading float64") {
    val df = sqlContext.read.hdf5(h5file, float64test)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", DoubleType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = (0 until 10).map(x => x % 2 match {
      case 0 => Row(x, (2 * x).toDouble / 10)
      case 1 => Row(x, (-2 * x).toDouble / 10)
    })

    checkEqual(df.drop("fileID"), expected)
  }

  test("Reading fixed length strings") {
    val dataset = "/datatypes/string"
    val alpha = "abcdefghijklmnopqrstuvwxyz"
    val df = sqlContext.read.hdf5(h5file, dataset)

    val expectedSchema = StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index0", LongType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )
    assert(df.schema === expectedSchema)

    val expected = (0 until 10).map { x => Row(x, alpha.substring(0, 0 + x)) }

    checkEqual(df.drop("fileID"), expected)
  }

  test("Testing 1d hyperslab") {
    val df = sqlContext.read.option("block", "3").option("start", "2").hdf5(h5file, int8test)

    val expectedSchema = StructType(Seq(
      StructField("fileID", IntegerType, nullable = false),
      StructField("index0", LongType, nullable = false),
      StructField("value", ByteType, nullable = false)
    ))
    assert(df.schema === expectedSchema)

    val len = df.drop("fileID").drop("index0").sort("value").collect
    val expect = Array(Row(0L), Row(1L), Row(2L))
    assert(len === expect)
  }

  val gfile = getClass.getResource("GSSTF_NCEP.h5").toString
  val ssttest = "/HDFEOS/GRIDS/NCEP/Data Fields/SST"

  test("Testing 2d hyperslab") {
    val df = sqlContext.read.option("block", "3,3").option("start", "2,2").hdf5(gfile, ssttest)
    val expectedSchema = StructType(Seq(
      StructField("fileID", IntegerType, nullable = false),
      StructField("index0", LongType, nullable = false),
      StructField("value", FloatType, nullable = false)
    ))
    assert(df.schema === expectedSchema)

    val len = df.drop("fileID").drop("value").sort("index0").collect
    val expect = Array(Row(2882L), Row(2883L), Row(2884L), Row(4322L), Row(4323L), Row(4324L),
      Row(5762L), Row(5763L), Row(5764L))
    assert(len === expect)
  }
}

