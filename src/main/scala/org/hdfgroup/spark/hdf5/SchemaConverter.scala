package org.hdfgroup.spark.hdf5

import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.slf4j.LoggerFactory

object SchemaConverter {

  private val log = LoggerFactory.getLogger(getClass)

  def convertSchema(dataset: ArrayVar[_]): StructType = {
    log.trace("{}", Array[AnyRef](dataset))

    dataset.path match {
      case "sparky://files" => {
        StructType(Array(
          StructField("FileID", IntegerType, false),
          StructField("FilePath", StringType, false),
          StructField("FileSize", LongType, false)
        ))
      }
      case "sparky://datasets" => {
        StructType(Array(
          StructField("FileID", IntegerType, false),
          StructField("DatasetPath", StringType, false),
          StructField("ElementType", StringType, false),
          StructField("Dimensions", ArrayType(LongType), false),
          StructField("ElementCount", LongType, false)
        ))

      }
      case "sparky://attributes" => {
        StructType(Array(
          StructField("FileID", IntegerType, false),
          StructField("ObjectPath", StringType, false),
          StructField("AttributeName", StringType, false),
          StructField("ElementType", StringType, false),
          StructField("Dimensions", ArrayType(LongType), false),
          StructField("Value", StringType, false)          
        ))
      }
      case _ => {
        val columns = StructType(Array(
          StructField("FileID", IntegerType, false),
          StructField("Index", LongType, false)
        ))
        StructType(columns :+ StructField("Value", extractTypes(dataset.contains), false))
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
      //case UInt64(_, _) => ?
      case Int64(_, _) => LongType
      case Float32(_, _) => FloatType
      case Float64(_, _) => DoubleType
      case FLString(_, _) => StringType
      case _ => throw new SparkException("Unsupported type found!")
    }
  }
}
