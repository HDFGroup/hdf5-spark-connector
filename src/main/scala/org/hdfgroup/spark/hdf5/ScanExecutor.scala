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
  case class UnboundedScan(dataset: ArrayVar[_], ioSize: Int) extends ScanItem
  case class BoundedScan(dataset: ArrayVar[_], ioSize: Int, blockNumber: Long = 0) extends ScanItem
  case class BoundedMDScan(dataset: ArrayVar[_], ioSize: Int, blockDimensions: Array[Int], offset: Array[Long]) extends ScanItem
  case class PrunedUnboundedScan(dataset: ArrayVar[_], ioSize: Int, requiredColumns: Array[String]) extends ScanItem
  case class PrunedBoundedScan(dataset: ArrayVar[_], ioSize: Int, blockNumber: Long = 0, requiredColumns: Array[String]) extends ScanItem
  case class PrunedBoundedMDScan(dataset: ArrayVar[_], ioSize: Int, blockDimensions: Array[Int], offset: Array[Long], requiredColumns: Array[String]) extends ScanItem
}

class ScanExecutor(filePath: String, fileID: Integer) extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def execQuery[T](scanItem: ScanItem): Seq[Row] = {
    log.trace("{}", Array[AnyRef](scanItem))

    scanItem match {
      case UnboundedScan(dataset, _) => dataset.path match {
        case "sparky://files" =>
          Seq(Row(dataset.fileID, dataset.fileName, dataset.realSize))

        case "sparky://datasets" =>
          val typeInfo = dataset.contains.toString
          Seq(Row(dataset.fileID, dataset.realPath,
            typeInfo.substring(0, typeInfo.indexOf('(')),
            dataset.dimension, dataset.realSize))

        case "sparky://attributes" =>
          val typeInfo = dataset.contains.toString
          Seq(Row(dataset.fileID, dataset.realPath, dataset.attribute,
            typeInfo.substring(0, typeInfo.indexOf('(')),
            dataset.dimension))

        case _ => {
          val dataReader = newDatasetReader(dataset)(_.readDataset())
          dataReader.zipWithIndex.map {
            case (x, index) =>
              Row(fileID, index.toLong, x)
          }
        }
      }

      case BoundedScan(dataset, ioSize, offset) => {
        val dataReader = newDatasetReader(dataset)(_.readDataset(ioSize, offset))
        dataReader.zipWithIndex.map {
          case (x, index) => Row(fileID, offset + index.toLong, x)
        }
      }

      case BoundedMDScan(dataset, ioSize, blockDimensions, offset) => {
        // Calculations to correctly map the index of each datapoint in
        // respect to the overall linearized matrix.
        val dataReader = newDatasetReader(dataset)(_.readDataset(blockDimensions, offset))
        val d = dataset.dimension
        val edgeBlockY =
          if ((offset(1) / blockDimensions(1)) < ((Math.floor(d(1) / blockDimensions(1))).toInt))
            blockDimensions(1)
          else
            d(1) % offset(1)
        val blockFill = offset(0) * d(1)
        dataReader.zipWithIndex.map {
          case (x, index) =>
            Row(fileID, blockFill + (index - index % edgeBlockY) / edgeBlockY
              * d(1) + index % edgeBlockY + offset(1), x)
        }
      }

      case PrunedUnboundedScan(dataset, _, cols) => dataset.path match {
        case "sparky://files" => {
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

        case "sparky://datasets" => {
          var listRows = List[Any]()
          val typeInfo = dataset.contains.toString
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

        case "sparky://attributes" => {
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

        case _ => {
          val dataReader = newDatasetReader(dataset)(_.readDataset())
          dataReader.zipWithIndex.map {
            case (x, index) => {
              var listRows = List[Any]()
              for (col <- cols) {
                if (col == "fileID")
                  listRows = listRows :+ fileID
                else if (col == "index0")
                  listRows = listRows :+ index.toLong
                else if (col == "value")
                  listRows = listRows :+ x
              }
              Row.fromSeq(listRows)
            }
          }
        }
      }

      case PrunedBoundedScan(dataset, ioSize, offset, cols) => {
        val dataReader = newDatasetReader(dataset)(_.readDataset(ioSize, offset))
        dataReader.zipWithIndex.map {
          case (x, index) => {
            var listRows = List[Any]()
            for (col <- cols) {
              if (col == "fileID")
                listRows = listRows :+ fileID
              else if (col == "index0")
                listRows = listRows :+ offset + index.toLong
              else if (col == "value")
                listRows = listRows :+ x
            }
            Row.fromSeq(listRows)
          }
        }
      }

      case PrunedBoundedMDScan(dataset, ioSize, blockDimensions, offset, cols) => {
        // Calculations to correctly map the index of each datapoint in
        // respect to the overall linearized matrix.
        val dataReader = newDatasetReader(dataset)(_.readDataset(blockDimensions, offset))
        val d = dataset.dimension
        val edgeBlockY =
          if ((offset(1) / blockDimensions(1)) < ((Math.floor(d(1) / blockDimensions(1))).toInt))
            blockDimensions(1)
          else
            d(1) % offset(1)
        val blockFill = offset(0) * d(1)
        dataReader.zipWithIndex.map {
          case (x, index) => {
            var listRows = List[Any]()
            for (col <- cols) {
              if (col == "fileID")
                listRows = listRows :+ fileID
              else if (col == "index0")
                listRows = listRows :+ (blockFill + (index - index % edgeBlockY) / edgeBlockY
                  * d(1) + index % edgeBlockY + offset(1))
              else if (col == "value")
                listRows = listRows :+ x
            }
            Row.fromSeq(listRows)
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
