package org.hdfgroup.spark.hdf5

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{max, sum}

class AttrTests extends FunTestSuite {

  val h5dir = FilenameUtils.getFullPathNoEndSeparator(
   getClass.getResource("root_group_attrs.h5").getPath)

  val file = getClass.getResource("root_group_attrs.h5").toString

  test("Reading sparky://attributes : root_group_attr.h5") {

    val df = spark.read.hdf5(file, vrtlAttributes)

    assert(df.schema === makeSchema(vrtlAttributes))

    val sortedVals = df.drop("FileID").drop("ObjectPath").drop("AttributeName")
    .drop("Dimensions").drop("ElementType").sort("Value").head(16)
    val expected = Array(
                         Row("-128,127"),
                         Row("-32768,0,1,32767"),                          
                         Row("0,255"), 
                         Row("1.79E308"), 
                         Row("1.8446744073709552E19"), 
                         Row("127"), 
                         Row("2147483647"), 
                         Row("255"),
                         Row("3.402823E38"),
                         Row("32767"),
                         Row("4294967295"), 
                         Row("65535"), 
                         Row("9223372036854775807"),
                         Row("abc,UNSUPPORTED"),  
                         Row("test"))
    assert(sortedVals === expected)
  }


}
