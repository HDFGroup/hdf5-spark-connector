package org.hdfgroup.spark.hdf5.reader

import org.apache.spark.SparkException
import org.slf4j.LoggerFactory

import ch.systemsx.cisd.hdf5.IHDF5Reader
import org.hdfgroup.spark.hdf5.reader.HDF5Schema.ArrayVar

class DatasetReader[T](val reader: IHDF5Reader, val node: ArrayVar[T]) extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def readDataset(): Array[T] =
    {
      log.trace("readDataset(): Array[T]", node.dimension.length)

      node.dimension.length match {
        case 1 => node.contains.readArray(reader)
        case 2 => node.contains.readMatrix(reader)
        case _ => throw new SparkException("Unsupported dataset rank!")
      }
    }

  def readDataset(blockSize: Int, offset: Long): Array[T] = {
    log.trace("readDataset(blockSize: Int, blockNumber: Long): Array[T]")

    node.contains.readArrayBlockWithOffset(reader, blockSize, offset)
  }

  def readDataset(blockSize: Array[Int], blockIndex: Array[Long]): Array[T] = {
    log.trace("readDataset(blockSize: Array[Int], blockIndex: Array[Long]): Array[T]")

    node.contains.readMatrixBlock(reader, blockSize, blockIndex)
  }
}
