package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ min, max, sum }
import org.apache.spark.sql.types._

class VRTLTests extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("test1.h5").getPath)

  test("Reading sparky://files : single file") {
    val df = sqlContext.read.hdf5(h5dir, vrtlFiles)

    assert(df.schema === makeSchema(vrtlFiles))

    val value = df.agg(max(df("file size"))).head
    val expected = Row(106671208.toLong)

    checkRowsEqual(value, expected)
  }

  test("Reading sparky://files : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, vrtlFiles)

    assert(df.schema === makeSchema(vrtlFiles))

    val totalSize = df.agg(sum(df("file size"))).head
    val expected = Row(123356044L)
    checkRowsEqual(totalSize, expected)
  }

  test("Reading sparky://datasets : single file") {
    val df = sqlContext.read.hdf5(h5dir, vrtlDatasets)

    assert(df.schema === makeSchema(vrtlDatasets))

    val totalCount = df.agg(sum(df("number of elements"))).head
    val expected = Row(69919900L)
    checkRowsEqual(totalCount, expected)
  }

  test("Reading sparky://datasets : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, vrtlDatasets)

    assert(df.schema === makeSchema(vrtlDatasets))

    val maxValue = df.agg(max(df("number of elements"))).head
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
    val df = sqlContext.read.hdf5(h5dir, vrtlAttributes)

    assert(df.schema === makeSchema(vrtlAttributes))

    val sortedVals = df.drop("fileID").drop("object path").drop("attribute name")
      .drop("dimensions").sort("element type").head()
    val expected = Row("FLString")
    assert(sortedVals === expected)
  }

  test("Reading sparky://attributes : multiple files") {
    val df = sqlContext.read.hdf5(h5dir, vrtlAttributes)

    assert(df.schema === makeSchema(vrtlAttributes))

    val sortedVals = df.drop("fileID").drop("object path").drop("element type")
      .drop("dimensions").sort("attribute name").head()
    val expected = Row("BeginDate")
    assert(sortedVals === expected)
  }

}
