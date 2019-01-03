package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{max, sum}

class VRTLTests extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
   getClass.getResource("test1.h5").getPath)

  test("Reading sparky://files : single file") {
    val df = spark.read.hdf5(h5dir, vrtlFiles)

    assert(df.schema === makeSchema(vrtlFiles))

    val value = df.agg(max(df("FileSize"))).head
    val expected = Row(106671208.toLong)

    checkRowsEqual(value, expected)
  }

  test("Reading sparky://files : multiple files") {
    val df = spark.read.hdf5(h5dir, vrtlFiles)

    assert(df.schema === makeSchema(vrtlFiles))

    val totalSize = df.agg(sum(df("FileSize"))).head
    val expected = Row(123368428L)
    checkRowsEqual(totalSize, expected)
  }

  test("Reading sparky://datasets : single file") {
    val df = spark.read.hdf5(h5dir, vrtlDatasets)

    assert(df.schema === makeSchema(vrtlDatasets))

    val totalCount = df.agg(sum(df("ElementCount"))).head
    val expected = Row(69921180L)
    checkRowsEqual(totalCount, expected)
  }

  test("Reading sparky://datasets : multiple files") {
    val df = spark.read.hdf5(h5dir, vrtlDatasets)

    assert(df.schema === makeSchema(vrtlDatasets))

    val maxValue = df.agg(max(df("ElementCount"))).head
    val expected = Row(7560000L)
    checkRowsEqual(maxValue, expected)

    val c = df.count()
    assert(c === 64)

    val len = df.drop("FileID").drop("DatasetPath").drop("ElementType")
      .drop("ElementCount").sort("Dimensions").head(2).apply(1).get(0)
    val expect = Array(2, 2, 5)
    assert(len === expect)
  }

  test("Reading sparky://attributes : single file") {
    val df = spark.read.hdf5(h5dir, vrtlAttributes)

    assert(df.schema === makeSchema(vrtlAttributes))

    val sortedVals = df.drop("FileID").drop("ObjectPath").drop("AttributeName")
      .drop("Dimensions").drop("Value").sort("ElementType").head()
    val expected = Row("FLString")
    assert(sortedVals === expected)
  }

  test("Reading sparky://attributes : multiple files") {
    val df = spark.read.hdf5(h5dir, vrtlAttributes)

    assert(df.schema === makeSchema(vrtlAttributes))

    val sortedVals = df.drop("FileID").drop("ObjectPath").drop("ElementType")
      .drop("Dimensions").drop("Value").sort("AttributeName").head()
    val expected = Row("BeginDate")
    assert(sortedVals === expected)
  }

}
