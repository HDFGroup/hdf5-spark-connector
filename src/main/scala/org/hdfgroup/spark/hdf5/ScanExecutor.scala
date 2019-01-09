// Copyright (C) 2017 The HDF Group
// All rights reserved.
//
//  \author Hyo-Kyung Lee (hyoklee@hdfgroup.org)
//  \date October 18, 2017
//  \note added SlicedMDScan() class case.
//
package org.hdfgroup.spark.hdf5

import java.io.File

import org.apache.spark.sql.Row
import org.hdfgroup.spark.hdf5.ScanExecutor._
import org.hdfgroup.spark.hdf5.reader.HDF5Schema.ArrayVar
import org.hdfgroup.spark.hdf5.reader.{DatasetReader, HDF5Reader}
import org.slf4j.LoggerFactory

import scala.language.existentials

object ScanExecutor {

  sealed trait ScanItem {
    val dataset: ArrayVar[_]
    val ioSize: Int
  }

  case class UnboundedScan(
    dataset: ArrayVar[_],
    ioSize: Int,
    cols: Array[String])
      extends ScanItem

  case class BoundedScan(
    dataset: ArrayVar[_],
    ioSize: Int,
    blockNumber: Long = 0,
    cols: Array[String])
      extends ScanItem

  case class BoundedMDScan(
    dataset: ArrayVar[_],
    ioSize: Int,
    blockDimensions: Array[Int],
    offset: Array[Long],
    cols: Array[String])
      extends ScanItem

  case class SlicedMDScan(
    dataset: ArrayVar[_],
    ioSize: Int,
    blockDimensions: Array[Int],
    offset: Array[Long],
    index: Array[Long],
    cols: Array[String])
      extends ScanItem

}

class ScanExecutor(filePath: String, fileID: Integer) extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  private val dataSchema = Array[String]("FileID", "Index", "Value")

  def openReader[T](fun: HDF5Reader => T): T = {
    log.trace("{}", Array[AnyRef](fun))

    val file = new File(filePath)
    val reader = new HDF5Reader(file, fileID)
    val result = fun(reader)
    reader.close()
    result
  }

  def newDatasetReader[S, T]
    (node: ArrayVar[T])(fun: DatasetReader[T] => S): S = {
    log.trace("{} {}", Array[AnyRef](node, fun))

    openReader(reader => reader.getDataset(node)(fun))
  }

  // TODO: This needs to be refactored.

  // Returns a sequence of the virtual table rows or the data/index/fileID rows.
  // The data rows are hard-coded to efficiently read the data.
  // PrunedScans with two columns must be checked to return the columns in the
  // correct order.
  def execQuery[T](scanItem: ScanItem): Seq[Row] = {
    log.trace("{}", Array[AnyRef](scanItem))

    scanItem match {

      //========================================================================
      // UnboundedScan
      //========================================================================

      case UnboundedScan(dataset, _, cols) => dataset.path match {

        // Check for virtual tables first

        case "sparky://files" => {
          if (cols.length == 0)
            Seq(Row(dataset.fileID, dataset.fileName, dataset.realSize))
          else {
            Seq(Row.fromSeq(for (col <- cols) yield {
              col match {
                case "FileID" => dataset.fileID
                case "FilePath" => dataset.fileName
                case "FileSize" => dataset.realSize
              }
            }))
          }
        }

        case "sparky://datasets" => {
          val reader = new HDF5Reader(new File(dataset.fileName), dataset.fileID)
          val nodes = reader.nodes.flatten()
          reader.close()

          nodes.collect {
            case d: ArrayVar[_] =>
              val typeInfo = dataset.contains.toString
              Row.fromSeq(for (col <- cols) yield {
                col match {
                  case "FileID" => d.fileID
                  case "DatasetPath" => d.realPath
                  case "ElementType" =>
                    typeInfo.substring(0, typeInfo.indexOf('('))
                  case "Dimensions" => d.dimension
                  case "ElementCount" => d.size
                }
              })
          }
        }

        case "sparky://attributes" => {
          val reader = new HDF5Reader(new File(dataset.fileName), dataset.fileID)
          val nodes = reader.attributes.flatten()
          reader.close()

          nodes.collect {
            case d: ArrayVar[_] =>
              val typeInfo = dataset.contains.toString
              Row.fromSeq(for (col <- cols) yield {
                col match {
                  case "FileID" => d.fileID
                  case "ObjectPath" => d.realPath
                  case "AttributeName" => d.attribute
                  case "ElementType" =>
                    typeInfo.substring(0, typeInfo.indexOf('('))
                  case "Dimensions" => d.dimension
                  case "Value" => d.value
                }
              })
          }
        }

        // "Real" scalar datasets

        case _ => {
          val col = if (cols.length == 0) dataSchema else cols
          val hasValue = col contains "Value"
          val hasIndex = col contains "Index"
          val hasFileID = col contains "FileID"

          if (hasValue) {
            val dataReader = newDatasetReader(dataset)(_.readDataset())
            if (hasIndex) {
              val indexed = dataReader.zipWithIndex

              // FIXME This does not cover the case that all three columns were
              //       specified out of order
              if (hasFileID)
                indexed.map { case (x, index) => Row(fileID, index.toLong, x) }
              else
                indexed.map {
                  case (x, index) =>
                    col(0) match {
                      case "Index" => Row(index.toLong, x)
                      case _ => Row(x, index.toLong)
                    }
                }
            }
            else {
              if (hasFileID)
                col(0) match {
                  case "FileID" => dataReader.map { x => Row(fileID, x) }
                  case _ => dataReader.map { x => Row(x, fileID) }
                }
                else dataReader.map { x => Row(x) }
            }
          }
          else {
            if (hasIndex) {
              val indexed = (0L until dataset.size)

              if (hasFileID)
                col(0) match {
                  case "FileID" => indexed.map { x => Row(fileID, x) }
                  case _ => indexed.map { x => Row(x, fileID) }
                }
                else
                  indexed.map { x => Row(x) }
            } else
                Seq(Row(fileID))
          }
        }

        // TODO Compound types

      } // case UnBoundedScan

      //========================================================================
      // BoundedScan (1D datasets)
      //========================================================================

      case BoundedScan(dataset, ioSize, offset, cols) => {
        val col = if (cols.length == 0) dataSchema else cols
        val hasValue = col contains "Value"
        val hasIndex = col contains "Index"
        val hasFileID = col contains "FileID"

        if (hasValue) {
          val dataReader = newDatasetReader(dataset)(
            _.readDataset(ioSize, offset))

          if (hasIndex) {
            val indexed = dataReader.zipWithIndex

            // FIXME This does not cover the case that all three columns were
            //       specified out of order
            if (hasFileID) indexed.map {
              case (x, index) => Row(fileID, offset + index.toLong, x)
            }
            else
              indexed.map {
                case (x, index) =>
                  col(0) match {
                    case "Index" => Row(offset + index.toLong, x)
                    case _ => Row(x, offset + index.toLong)
                  }
              }
          }
          else {
            if (hasFileID)
              dataReader.map {
                x => col(0) match {
                  case "FileID" => Row(fileID, x)
                  case _ => Row(x, fileID)
                }
              }
              else
                dataReader.map { x => Row(x) }
          }
        }
        else {
          if (hasIndex) {
            val indexed = (0L until dataset.size)

            if (hasFileID)
              indexed.map {
                x => col(0) match {
                  case "FileID" => Row(fileID, offset + x.toLong)
                  case _ => Row(offset + x.toLong, fileID)
                }
              }
              else
                indexed.map { x => Row(offset + x.toLong) }
          }
          else
            Seq(Row(fileID))
        }
      } // BoundedScan

      //========================================================================
      // BoundedMDScan (2D+ datasets)
      //========================================================================

      case BoundedMDScan(dataset, ioSize, blockDimensions, offset, cols) => {
        val col = if (cols.length == 0) dataSchema else cols

        val hasValue = col contains "Value"
        val hasIndex = col contains "Index"
        val hasFileID = col contains "FileID"

        val d = dataset.dimension
        val edgeBlock = (offset, blockDimensions, d).zipped.map {
          case (offset, dim, d) => {
            if ((offset / dim) < ((Math.floor(d / dim)).toInt)) dim
            else d % offset
          }
        }
        val blockFill = offset(0) * d(1)
        if (hasValue) {
          // Calculations to correctly map the index of each datapoint in
          // respect to the overall linearized matrix.
          val dataReader = newDatasetReader(dataset)(
            _.readDataset(blockDimensions, offset))

          if (hasIndex) {
            val indexed = dataReader.zipWithIndex

            // FIXME This does not cover the case that all three columns were
            //       specified out of order

            if (hasFileID) indexed.map {
              case (x, index) =>
                Row(fileID,
                  blockFill + (index - index % edgeBlock(1)) /
                    edgeBlock(0) * d(1) + index % edgeBlock(1) + offset(1), x)
            }
            else {
              indexed.map {
                case (x, index) => {
                  val globalIndex = blockFill +
                  (index - index % edgeBlock(1)) / edgeBlock(1) * d(1)
                  + index % edgeBlock(1) + offset(1)
                  col(0) match {
                    case "Index" => Row(globalIndex, x)
                    case _ => Row(x, globalIndex)
                  }
                }
              }
            }
          }
          else {
            if (hasFileID)
              dataReader.map {
                x => col(0) match {
                  case "FileID" => Row(fileID, x)
                  case _ => Row(x, fileID)
                }
              }
              else dataReader.map { x => Row(x) }
          }
        }
        else {
          if (hasIndex) {
            val indexed = (0L until edgeBlock(0) * edgeBlock(1).toLong)
            if (hasFileID)
              indexed.map {
                x => {
                  val globalIndex = blockFill +
                  (x - x % edgeBlock(1)) / edgeBlock(1) * d(1) +
                  x % edgeBlock(1) + offset(1)
                  col(0) match {
                    case "FileID" => Row(fileID, globalIndex)
                    case _ => Row(globalIndex, fileID)
                  }
                }
              }
            else {
              indexed.map {
                x => Row(blockFill +
                  (x - x % edgeBlock(1)) / edgeBlock(1) * d(1) +
                  x % edgeBlock(1) + offset(1))
              }
            }
          }
          else
            Seq(Row(fileID))
        }
      } // BoundedMDScan

      //========================================================================
      // SlicedMDScan
      //========================================================================

      case SlicedMDScan(
        dataset,
        ioSize,
        blockDimensions,
        offset,
        index,
        cols) => {
        val dataReader = newDatasetReader(dataset)(
          _.readDataset(blockDimensions, offset, index))
        // This is not complete yet. <hyokyung 2017.10.18. 08:06:06>
        // The goal is to test whether readSlicedMDArrayBlockWithOffset()
        // function in HDF5Schema.scala return the right subsetted array of
        // integers.
        dataReader.map {
          x => val l:Long=x.asInstanceOf[Number].longValue;  Row(l)
        }
      } // SlicedMDScan
    }
  }
}
