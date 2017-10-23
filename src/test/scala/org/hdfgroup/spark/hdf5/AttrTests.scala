package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{max, sum}

class AttrTests extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
   getClass.getResource("root_group_attrs.h5").getPath)

  val file = getClass.getResource("root_group_attrs.h5").toString

  test("Reading sparky://attributes : root_group_attr.h5") {

    val df = sqlContext.read.hdf5(file, vrtlAttributes)

    assert(df.schema === makeSchema(vrtlAttributes))

    val sortedVals = df.drop("FileID").drop("ObjectPath").drop("AttributeName")
      .drop("Dimensions").drop("ElementType").head(3)
    val expected = Array(Row("test"), Row("127"), Row("255"))
    assert(sortedVals === expected)
  }


}
