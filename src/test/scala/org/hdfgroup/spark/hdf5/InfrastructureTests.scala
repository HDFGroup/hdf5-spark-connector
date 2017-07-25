package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ min, max }
import org.apache.spark.sql.types._

class InfrastructureTests extends FunTestSuite {

  val h5file = getClass.getResource("test1.h5").toString
  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
    getClass.getResource("test1.h5").getPath)

  val multiDataset = "/multi"

  test("Reading multiple files") {
    val df = sqlContext.read.hdf5(h5dir, multiDataset).drop("fileID")
    val expected = (0 until 30).map { x => Row(x % 10, x) }

    checkEqual(df, expected)
  }

  val int8test = "/datatypes/int8"
  
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

  /*
  val path = "/Users/Alan/IdeaProjects/5parky/examples/GSSTF_NCEP_mini"

  test("Testing non-recursion") {
    val df = sqlContext.read.option("recursion", "false").
      option("extension", "he5").hdf5(path, ssttest)
    val df2 = sqlContext.read.option("extension", "he5").hdf5(path, ssttest)
    val expectedSchema = StructType(Seq(
      StructField("fileID", IntegerType, nullable = false),
      StructField("index0", LongType, nullable = false),
      StructField("value", FloatType, nullable = false)
    ))
    assert(df.schema === expectedSchema)

    assert(df.count()*3 === df2.count())
  }
   */
}

