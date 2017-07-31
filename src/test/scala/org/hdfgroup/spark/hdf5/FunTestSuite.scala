package org.hdfgroup.spark.hdf5

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/*
 * Base abstract class for all unit tests in Spark for handling common functionality.
 */
abstract class FunTestSuite extends FunSuite with BeforeAndAfterAll {

  private val sparkConf = new SparkConf()

  protected var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "HDF5Suite", sparkConf))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  val vrtlAttributes = "sparky://attributes"

  val vrtlDatasets = "sparky://datasets"

  val vrtlFiles = "sparky://files"

  def makeSchema(valueType: DataType): StructType = {
    StructType(
      Seq(
        StructField("fileID", IntegerType, nullable = false),
        StructField("index", LongType, nullable = false),
        StructField("value", valueType, nullable = false)))
  }

  def makeSchema(vrtlPath: String): StructType = {
    vrtlPath match {
      case "sparky://attributes" =>
        StructType(
          Seq(
            StructField("fileID", IntegerType, nullable = false),
            StructField("object path", StringType, nullable = false),
            StructField("attribute name", StringType, nullable = false),
            StructField("element type", StringType, nullable = false),
            StructField("dimensions", ArrayType(LongType), nullable = false)))
      case "sparky://datasets" =>
        StructType(
          Seq(
            StructField("fileID", IntegerType, nullable = false),
            StructField("dataset name", StringType, nullable = false),
            StructField("element type", StringType, nullable = false),
            StructField("dimensions", ArrayType(LongType), nullable = false),
            StructField("number of elements", LongType, nullable = false)))
      case "sparky://files" =>
        StructType(
          Seq(
            StructField("fileID", IntegerType, nullable = false),
            StructField("fileName", StringType, nullable = false),
            StructField("file size", LongType, nullable = false)))
    }
  }

  def checkEqual(df: DataFrame, expected: Seq[Row]): Unit = {
    assert(df.collect.toSet === expected.toSet)
  }

  def checkRowsEqual(row: Row, expected: Row): Unit = {
    assert(row == expected)
  }

}
