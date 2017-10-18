// Copyright (C) 2017 The HDF Group
// All rights reserved.
//
//  \author Hyo-Kyung Lee (hyoklee@hdfgroup.org)
//  \date October 18, 2017
//  \note added multi-dimensional case readDataset() function.
//  \note cleaned up log message to match parameters.
package org.hdfgroup.spark.hdf5.reader
import ch.systemsx.cisd.hdf5.IHDF5Reader
import org.apache.spark.SparkException
import org.hdfgroup.spark.hdf5.reader.HDF5Schema.ArrayVar
import org.slf4j.LoggerFactory

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
    log.trace("readDataset(blockSize: Int, offset: Long): Array[T]")

    node.contains.readArrayBlockWithOffset(reader, blockSize, offset)
  }

  def readDataset(blockSize: Array[Int], offset: Array[Long]): Array[T] = {
    log.trace("readDataset(blockSize: Array[Int], offset: Array[Long]): Array[T]")

    node.contains.readMatrixBlockWithOffset(reader, blockSize, offset)
  }

  def readDataset(blockSize: Array[Int], offset: Array[Long], index: Array[Long]):
      Array[T] = {
      log.trace("readDataset(blockSize: Array[Int], offset: Array[Long], index: Array[Long]): Array[T]")
      node.contains.readSlicedMDArrayBlockWithOffset(reader, blockSize, offset, index)
  }      
}
