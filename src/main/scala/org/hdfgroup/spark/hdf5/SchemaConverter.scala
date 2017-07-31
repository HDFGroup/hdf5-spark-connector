package org.hdfgroup.spark.hdf5

import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.apache.spark.sql.types._
import org.apache.spark.SparkException
import org.slf4j.LoggerFactory

object SchemaConverter {

  private val log = LoggerFactory.getLogger(getClass)

  def convertSchema(dataset: ArrayVar[_]): StructType = {
    log.trace("{}", Array[AnyRef](dataset))

    dataset.path match {
      case "sparky://files" => {
        StructType(Array(
          StructField("fileID", IntegerType, false),
          StructField("fileName", StringType, false),
          StructField("file size", LongType, false)
        ))
      }
      case "sparky://datasets" => {
        StructType(Array(
          StructField("fileID", IntegerType, false),
          StructField("dataset name", StringType, false),
          StructField("element type", StringType, false),
          StructField("dimensions", ArrayType(LongType), false),
          StructField("number of elements", LongType, false)
        ))

      }
      case "sparky://attributes" => {
        StructType(Array(
          StructField("fileID", IntegerType, false),
          StructField("object path", StringType, false),
          StructField("attribute name", StringType, false),
          StructField("element type", StringType, false),
          StructField("dimensions", ArrayType(LongType), false)
        ))
      }
      case _ => {
        val columns = StructType(Array(
          StructField("fileID", IntegerType, false),
          StructField("index", LongType, false)
        ))
        StructType(columns :+ StructField("value", extractTypes(dataset.contains), false))
        /* For now we linearize everything
        val columns = dataset.dimension.indices.map {
        index => "index" + index
        }.map {
        name => StructField(name, LongType, nullable = false)
        }
        */
      }
    }
  }

  def extractTypes(datatype: HDF5Type[_]): DataType = {
    log.trace("{}", Array[AnyRef](datatype))

    datatype match {
      case Int8(_, _) => ByteType
      case UInt8(_, _) => ShortType
      case Int16(_, _) => ShortType
      case UInt16(_, _) => IntegerType
      case Int32(_, _) => IntegerType
      case UInt32(_, _) => LongType
      case Int64(_, _) => LongType
      case Float32(_, _) => FloatType
      case Float64(_, _) => DoubleType
      case FLString(_, _) => StringType
    }
  }
}
