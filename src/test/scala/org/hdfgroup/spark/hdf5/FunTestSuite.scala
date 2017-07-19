package org.hdfgroup.spark.hdf5

import org.apache.spark.sql._
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

  def checkEqual(df: DataFrame, expected: Seq[Row]): Unit = {
    assert(df.collect.toSet === expected.toSet)
  }

  def checkRowsEqual(row: Row, expected: Row): Unit = {
    assert(row == expected)
  }

}
