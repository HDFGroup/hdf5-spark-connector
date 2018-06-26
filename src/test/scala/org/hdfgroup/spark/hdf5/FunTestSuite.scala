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

  protected var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("HDF5Suite")
      .config(sparkConf)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
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
        StructField("FileID", IntegerType, nullable = false),
        StructField("Index", LongType, nullable = false),
        StructField("Value", valueType, nullable = false)))
  }

  def makeSchema(vrtlPath: String): StructType = {
    vrtlPath match {
      case "sparky://attributes" =>
        StructType(
          Seq(
            StructField("FileID", IntegerType, nullable = false),
            StructField("ObjectPath", StringType, nullable = false),
            StructField("AttributeName", StringType, nullable = false),
            StructField("ElementType", StringType, nullable = false),
            StructField("Dimensions", ArrayType(LongType), nullable = false),
            StructField("Value", StringType, nullable = false)))            
      case "sparky://datasets" =>
        StructType(
          Seq(
            StructField("FileID", IntegerType, nullable = false),
            StructField("DatasetPath", StringType, nullable = false),
            StructField("ElementType", StringType, nullable = false),
            StructField("Dimensions", ArrayType(LongType), nullable = false),
            StructField("ElementCount", LongType, nullable = false)))
      case "sparky://files" =>
        StructType(
          Seq(
            StructField("FileID", IntegerType, nullable = false),
            StructField("FilePath", StringType, nullable = false),
            StructField("FileSize", LongType, nullable = false)))
    }
  }

  def checkEqual(df: DataFrame, expected: Seq[Row]): Unit = {
    assert(df.collect.toSet === expected.toSet)
  }

  def checkRowsEqual(row: Row, expected: Row): Unit = {
    assert(row == expected)
  }

}
