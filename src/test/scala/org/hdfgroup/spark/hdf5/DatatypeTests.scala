package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class DatatypeTests extends FunTestSuite {

  val h5file = getClass.getResource("test1.h5").toString

  val int8test = "/datatypes/int8"
  val int16test = "/datatypes/int16"
  val int32test = "/datatypes/int32"
  val int64test = "/datatypes/int64"
  val uint8test = "/datatypes/uint8"
  val uint16test = "/datatypes/uint16"
  val uint32test = "/datatypes/uint32"
  val float32test = "/datatypes/float32"
  val float64test = "/datatypes/float64"

  // Signed integer tests

  test("Reading int8") {
    val df = spark.read.hdf5(h5file, int8test)

    assert(df.schema === makeSchema(ByteType))

    val expected = Row(0L, Byte.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toByte) } :+
      Row(9L, Byte.MaxValue)

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading int16") {
    val df = spark.read.hdf5(h5file, int16test)

    assert(df.schema === makeSchema(ShortType))

    val expected = Row(0L, Short.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toShort) } :+
      Row(9L, Short.MaxValue)

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading int32") {
    val df = spark.read.hdf5(h5file, int32test)

    assert(df.schema === makeSchema(IntegerType))

    val expected = Row(0L, Int.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toInt) } :+
      Row(9L, Int.MaxValue)

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading int64") {
    val df = spark.read.hdf5(h5file, int64test)

    assert(df.schema === makeSchema(LongType))

    val expected = Row(0L, Long.MinValue) +:
      (1L until 9L).map { x => Row(x, x - 2) } :+
      Row(9L, Long.MaxValue)

    checkEqual(df.drop("FileID"), expected)
  }

  // Unsigned integer tests

  test("Reading uint8") {
    val df = spark.read.hdf5(h5file, uint8test)

    assert(df.schema === makeSchema(ShortType))

    val expected = (0L until 9L).map { x => Row(x, x.toShort) } :+ Row(9L, 255)

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading uint16") {
    val df = spark.read.hdf5(h5file, uint16test)

    assert(df.schema === makeSchema(IntegerType))

    val expected = (0L until 9L).map { x => Row(x, x.toInt) } :+ Row(9L, 65535)

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading uint32") {
    val df = spark.read.hdf5(h5file, uint32test)

    assert(df.schema === makeSchema(LongType))

    val expected = (0L until 9L).map { x => Row(x, x) } :+ Row(9L, 4294967295L)

    checkEqual(df.drop("FileID"), expected)
  }

  // Float tests

  test("Reading float32") {
    val df = spark.read.hdf5(h5file, float32test)

    assert(df.schema === makeSchema(FloatType))

    val expected = (0 until 10).map(x => x % 2 match {
      case 0 => Row(x, (0.2 * x).toFloat)
      case 1 => Row(x, (-0.2 * x).toFloat)
    })

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading float64") {
    val df = spark.read.hdf5(h5file, float64test)

    assert(df.schema === makeSchema(DoubleType))

    val expected = (0 until 10).map(x => x % 2 match {
      case 0 => Row(x, (2 * x).toDouble / 10)
      case 1 => Row(x, (-2 * x).toDouble / 10)
    })

    checkEqual(df.drop("FileID"), expected)
  }

  test("Reading fixed length strings") {
    val dataset = "/datatypes/string"
    val alpha = "abcdefghijklmnopqrstuvwxyz"
    val df = spark.read.hdf5(h5file, dataset)

    assert(df.schema === makeSchema(StringType))

    val expected = (0 until 10).map { x => Row(x, alpha.substring(0, 0 + x)) }

    checkEqual(df.drop("FileID"), expected)
  }

}
