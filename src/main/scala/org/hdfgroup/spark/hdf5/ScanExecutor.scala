package org.hdfgroup.spark.hdf5

import java.io.File

import org.apache.spark.sql.Row
import org.hdfgroup.spark.hdf5.ScanExecutor._
import org.hdfgroup.spark.hdf5.reader.HDF5Schema.ArrayVar
import org.hdfgroup.spark.hdf5.reader.{ DatasetReader, HDF5Reader }
import org.slf4j.LoggerFactory

import scala.language.existentials

object ScanExecutor {
  sealed trait ScanItem {
    val dataset: ArrayVar[_]
    val ioSize: Int
  }
  case class UnboundedScan(dataset: ArrayVar[_], ioSize: Int, cols: Array[String]) extends ScanItem
  case class BoundedScan(dataset: ArrayVar[_], ioSize: Int, blockNumber: Long = 0, cols: Array[String]) extends ScanItem
  case class BoundedMDScan(dataset: ArrayVar[_], ioSize: Int, blockDimensions: Array[Int], offset: Array[Long], cols: Array[String]) extends ScanItem
}

class ScanExecutor(filePath: String, fileID: Integer) extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def execQuery[T](scanItem: ScanItem): Seq[Row] = {
    log.trace("{}", Array[AnyRef](scanItem))

    scanItem match {
      case UnboundedScan(dataset, _, cols) => dataset.path match {
        case "sparky://files" => {
          if (cols.length == 0)
            Seq(Row(dataset.fileID, dataset.fileName, dataset.realSize))
          else {
            var listRows = List[Any]()
            for (col <- cols) {
              if (col == "fileID")
                listRows = listRows :+ dataset.fileID
              else if (col == "fileName")
                listRows = listRows :+ dataset.fileName
              else if (col == "file size")
                listRows = listRows :+ dataset.realSize
            }
            Seq(Row.fromSeq(listRows))
          }
        }

        case "sparky://datasets" => {
          val typeInfo = dataset.contains.toString
          if (cols.length == 0)
            Seq(Row(dataset.fileID, dataset.realPath,
              typeInfo.substring(0, typeInfo.indexOf('(')),
              dataset.dimension, dataset.realSize))
          else {
            var listRows = List[Any]()
            for (col <- cols) {
              if (col == "fileID")
                listRows = listRows :+ dataset.fileID
              else if (col == "dataset name")
                listRows = listRows :+ dataset.realPath
              else if (col == "element type")
                listRows = listRows :+ typeInfo.substring(0, typeInfo.indexOf('('))
              else if (col == "dimensions")
                listRows = listRows :+ dataset.dimension
              else if (col == "number of elements")
                listRows = listRows :+ dataset.realSize
            }
            Seq(Row.fromSeq(listRows))
          }
        }

        case "sparky://attributes" => {
          val typeInfo = dataset.contains.toString
          if (cols.length == 0)
            Seq(Row(dataset.fileID, dataset.realPath, dataset.attribute,
              typeInfo.substring(0, typeInfo.indexOf('(')),
              dataset.dimension))
          else {
            var listRows = List[Any]()
            val typeInfo = dataset.contains.toString
            for (col <- cols) {
              if (col == "fileID")
                listRows = listRows :+ dataset.fileID
              else if (col == "object path")
                listRows = listRows :+ dataset.realPath
              else if (col == "attribute name")
                listRows = listRows :+ dataset.attribute
              else if (col == "element type")
                listRows = listRows :+ typeInfo.substring(0, typeInfo.indexOf('('))
              else if (col == "dimensions")
                listRows = listRows :+ dataset.dimension
            }
            Seq(Row.fromSeq(listRows))
          }
        }

        case _ => {
          val col =
            if (cols.length == 0) Array[String]("value", "index0", "fileID")
            else cols
          val hasValue = col contains "value"
          val hasIndex = col contains "index0"
          val hasID = col contains "fileID"
          if (hasValue) {
            val dataReader = newDatasetReader(dataset)(_.readDataset())
            if (hasIndex) {
              val indexed = dataReader.zipWithIndex
              if (hasID) indexed.map { case (x, index) => Row(fileID, index.toLong, x) }
              else indexed.map { case (x, index) => Row(index.toLong, x) }
            } else {
              if (hasID) dataReader.map { x => Row(fileID, x) }
              else dataReader.map { x => Row(x) }
            }
          } else {
            if (hasIndex) {
              val indexed = (0L until dataset.size)
              if (hasID) indexed.map { x => Row(fileID, x) }
              else indexed.map { x => Row(x) }
            } else {
              if (hasID) Seq(Row(fileID))
              else Seq(Row())
            }
          }
        }
      }

      case BoundedScan(dataset, ioSize, offset, cols) => {
        val col =
          if (cols.length == 0) Array[String]("value", "index0", "fileID")
          else cols
        val hasValue = col contains "value"
        val hasIndex = col contains "index0"
        val hasID = col contains "fileID"
        if (hasValue) {
          val dataReader = newDatasetReader(dataset)(_.readDataset(ioSize, offset))
          if (hasIndex) {
            val indexed = dataReader.zipWithIndex
            if (hasID) indexed.map { case (x, index) => Row(fileID, offset + index.toLong, x) }
            else {
              indexed.map { case (x, index) => {
                if (col(0) == "index0") Row(offset + index.toLong, x)
                else Row(x, offset + index.toLong)
              }}
            }
          } else {
            if (hasID) dataReader.map { x => {
              if (col(0) == "fileID") Row(fileID, x)
              else Row(x, fileID)
            }}
            else dataReader.map { x => Row(x) }
          }
        } else {
          if (hasIndex) {
            val indexed = (0L until dataset.size)
            if (hasID) indexed.map { x => {
              if (col(0) == "fileID") Row(fileID, offset + x.toLong)
              else Row(offset + x.toLong, fileID)
            }}
            else indexed.map { x => Row(offset + x.toLong) }
          } else {
            if (hasID) Seq(Row(fileID))
            else Seq(Row())
          }
        }
      }

      case BoundedMDScan(dataset, ioSize, blockDimensions, offset, cols) => {
        val col =
          if (cols.length == 0) Array[String]("value", "index0", "fileID")
          else cols
        val hasValue = col contains "value"
        val hasIndex = col contains "index0"
        val hasID = col contains "fileID"
        val d = dataset.dimension
        val edgeBlockX =
          if ((offset(0) / blockDimensions(0)) < ((Math.floor(d(0) / blockDimensions(0))).toInt))
            blockDimensions(0)
          else
            d(0) % offset(0)
        val edgeBlockY =
          if ((offset(1) / blockDimensions(1)) < ((Math.floor(d(1) / blockDimensions(1))).toInt))
            blockDimensions(1)
          else
            d(1) % offset(1)
        val blockFill = offset(0) * d(1)
        if (hasValue) {
          // Calculations to correctly map the index of each datapoint in
          // respect to the overall linearized matrix.
          val dataReader = newDatasetReader(dataset)(_.readDataset(blockDimensions, offset))
          if (hasIndex) {
            val indexed = dataReader.zipWithIndex
            if (hasID) indexed.map { case (x, index) => Row(fileID, blockFill + (index - index % edgeBlockY)
              / edgeBlockY * d(1) + index % edgeBlockY + offset(1), x)
            }
            else {
              indexed.map { case (x, index) => {
                val globalIndex = blockFill + (index - index % edgeBlockY) / edgeBlockY * d(1) + index % edgeBlockY + offset(1)
                if (col(0) == "index0") Row(globalIndex, x)
                else Row(x, globalIndex)
              }}
            }
          } else {
            if (hasID) dataReader.map { x => {
              if (col(0) == "fileID") Row(fileID, x)
              else Row(x, fileID)
            }
            }
            else dataReader.map { x => Row(x) }
          }
        } else {
          if (hasIndex) {
            val indexed = (0L until edgeBlockX * edgeBlockY.toLong)
            if (hasID) indexed.map { x => {
              val globalIndex = blockFill + (x - x % edgeBlockY) / edgeBlockY * d(1) + x % edgeBlockY + offset(1)
              if (col(0) == "fileID") Row(fileID, globalIndex)
              else Row(globalIndex, fileID)
            }}
            else {
              indexed.map { x => Row(blockFill + (x - x % edgeBlockY) / edgeBlockY * d(1) + x % edgeBlockY + offset(1)) }
            }
          } else {
            if (hasID) Seq(Row(fileID))
            else Seq(Row())
          }
        }
      }
    }
  }

  def openReader[T](fun: HDF5Reader => T): T = {
    log.trace("{}", Array[AnyRef](fun))

    val file = new File(filePath)
    val reader = new HDF5Reader(file, fileID)
    val result = fun(reader)
    reader.close()
    result
  }

  def newDatasetReader[S, T](node: ArrayVar[T])(fun: DatasetReader[T] => S): S = {
    log.trace("{} {}", Array[AnyRef](node, fun))

    openReader(reader => reader.getDataset(node)(fun))
  }
}
