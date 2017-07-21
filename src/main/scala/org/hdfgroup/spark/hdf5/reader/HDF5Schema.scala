package org.hdfgroup.spark.hdf5.reader

import org.apache.spark.SparkException

import ch.systemsx.cisd.hdf5._

object HDF5Schema {

  // TODO: Needs reference, time, unsigned, compound, enumeration
  //          case COMPOUND
  sealed trait HDF5Type[T] {
    def readArray(reader: IHDF5Reader): Array[T]
    def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[T]
    def readMatrix(reader: IHDF5Reader): Array[T]
    def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[T]
  }

  case class Int8(fileID: Integer, name: String) extends HDF5Type[Byte] {
    override def readArray(reader: IHDF5Reader): Array[Byte] =
      reader.int8.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Byte] =
      reader.int8.readArrayBlockWithOffset(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[Byte] =
      reader.int8.readMatrix(name).flatten

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Byte] =
      reader.int8.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten
  }

  case class UInt8(fileID: Integer, name: String) extends HDF5Type[Short] {
    override def readArray(reader: IHDF5Reader): Array[Short] =
      reader.uint8.readArray(name).map(UnsignedIntUtils.toUint8)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Short] =
      reader.uint8.readArrayBlockWithOffset(name, blockSize, offset).map(UnsignedIntUtils.toUint8)

    override def readMatrix(reader: IHDF5Reader): Array[Short] =
      reader.uint8.readMatrix(name).flatten.map(UnsignedIntUtils.toUint8)

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Short] =
      reader.uint8.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten.map(UnsignedIntUtils.toUint8)
  }

  case class Int16(fileID: Integer, name: String) extends HDF5Type[Short] {
    override def readArray(reader: IHDF5Reader): Array[Short] =
      reader.int16.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Short] =
      reader.int16.readArrayBlockWithOffset(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[Short] =
      reader.int16.readMatrix(name).flatten

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Short] =
      reader.int16.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten
  }

  case class UInt16(fileID: Integer, name: String) extends HDF5Type[Int] {
    override def readArray(reader: IHDF5Reader): Array[Int] =
      reader.uint16.readArray(name).map(UnsignedIntUtils.toUint16)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Int] =
      reader.uint16.readArrayBlockWithOffset(name, blockSize, offset).map(UnsignedIntUtils.toUint16)

    override def readMatrix(reader: IHDF5Reader): Array[Int] =
      reader.uint16.readMatrix(name).flatten.map(UnsignedIntUtils.toUint16)

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Int] =
      reader.uint16.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten.map(UnsignedIntUtils.toUint16)
  }

  case class Int32(fileID: Integer, name: String) extends HDF5Type[Int] {
    override def readArray(reader: IHDF5Reader): Array[Int] =
      reader.int32.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Int] =
      reader.int32.readArrayBlock(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[Int] =
      reader.int32.readMatrix(name).flatten

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Int] =
      reader.int32.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten
  }

  case class UInt32(ffileID: Integer, name: String) extends HDF5Type[Long] {
    override def readArray(reader: IHDF5Reader): Array[Long] =
      reader.uint32.readArray(name).map(UnsignedIntUtils.toUint32)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Long] =
      reader.uint32.readArrayBlockWithOffset(name, blockSize, offset).map(UnsignedIntUtils.toUint32)

    override def readMatrix(reader: IHDF5Reader): Array[Long] =
      reader.uint32.readMatrix(name).flatten.map(UnsignedIntUtils.toUint32)

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Long] =
      reader.uint32.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten.map(UnsignedIntUtils.toUint32)
  }

  case class Int64(fileID: Integer, name: String) extends HDF5Type[Long] {
    override def readArray(reader: IHDF5Reader): Array[Long] =
      reader.int64.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Long] =
      reader.int64.readArrayBlockWithOffset(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[Long] =
      reader.int64.readMatrix(name).flatten

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Long] =
      reader.int64.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten
  }

  case class Float32(fileID: Integer, name: String) extends HDF5Type[Float] {
    override def readArray(reader: IHDF5Reader): Array[Float] =
      reader.float32.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Float] =
      reader.float32.readArrayBlockWithOffset(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[Float] =
      reader.float32.readMatrix(name).flatten

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Float] =
      reader.float32.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten
  }

  case class Float64(fileID: Integer, name: String) extends HDF5Type[Double] {
    override def readArray(reader: IHDF5Reader): Array[Double] =
      reader.float64.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[Double] =
      reader.float64.readArrayBlockWithOffset(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[Double] =
      reader.float64.readMatrix(name).flatten

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[Double] =
      reader.float64.readMatrixBlockWithOffset(name, blockSize(0), blockSize(1), offset(0), offset(1)).flatten
  }

  case class FLString(fileID: Integer, name: String) extends HDF5Type[String] {
    override def readArray(reader: IHDF5Reader): Array[String] =
      reader.string.readArray(name)

    override def readArrayBlockWithOffset(reader: IHDF5Reader, blockSize: Int, offset: Long): Array[String] =
      reader.string.readArrayBlock(name, blockSize, offset)

    override def readMatrix(reader: IHDF5Reader): Array[String] = {
      throw new SparkException("'readMatrix' does not support strings.")
      Array[String]("")
    }

    override def readMatrixBlockWithOffset(reader: IHDF5Reader, blockSize: Array[Int], offset: Array[Long]): Array[String] = {
      throw new SparkException("'readMatrixBlock' does not support strings.")
      Array[String]("")
    }
  }

  sealed trait HDF5Node {
    val fileID: Integer
    val path: String

    def flatten(): Seq[HDF5Node]
  }

  case class ArrayVar[T](fileName: String, fileID: Integer, path: String, contains: HDF5Type[T], dimension: Array[Long], size: Long, realPath: String = null, realSize: Long = 0L, attribute: String) extends HDF5Node with Serializable {
    def flatten(): Seq[HDF5Node] = Seq(this)
  }

  case class Group(fileID: Integer, path: String, children: Seq[HDF5Node]) extends HDF5Node {
    def flatten(): Seq[HDF5Node] = this +: children.flatMap(x => x.flatten())
  }

  case class GenericNode(fileID: Integer, path: String) extends HDF5Node {
    def flatten(): Seq[HDF5Node] = Seq(this)
  }

}
