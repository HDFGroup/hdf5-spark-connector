package org.hdfgroup.spark.hdf5

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class HyperslabTests extends FunTestSuite {

  val h5file = getClass.getResource("test1.h5").toString

  val int8test = "/datatypes/int8"
  test("Testing 1d hyperslab") {
    val df = sqlContext.read.option("window size", "2").option("block", "3").
      option("start", "2").hdf5(h5file, int8test)

    assert(df.schema === makeSchema(ByteType))

    val len = df.drop("FileID").drop("Index").sort("Value").collect
    val expect = Array(Row(0L), Row(1L), Row(2L))
    assert(len === expect)
  }

  val gfile = getClass.getResource("GSSTF_NCEP.h5").toString
  val ssttest = "/HDFEOS/GRIDS/NCEP/Data Fields/SST"

  test("Testing 2d hyperslab") {
    val df = sqlContext.read.option("window size", "5").option("block", "3,3").
      option("start", "2,2").hdf5(gfile, ssttest)

    assert(df.schema === makeSchema(FloatType))

    val len = df.drop("FileID").drop("Value").sort("Index").collect
    val expect = Array(Row(2882L), Row(2883L), Row(2884L), Row(4322L), Row(4323L), Row(4324L),
      Row(5762L), Row(5763L), Row(5764L))
    assert(len === expect)
  }
}
