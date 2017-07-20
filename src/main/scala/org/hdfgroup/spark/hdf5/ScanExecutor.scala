package org.hdfgroup.spark.hdf5

import java.io.File

import scala.language.existentials
import scala.math._
import ch.systemsx.cisd.hdf5.{ HDF5DataClass, HDF5DataTypeInformation, HDF5FactoryProvider, IHDF5Reader }
import org.hdfgroup.spark.hdf5.ScanExecutor.{ BoundedMDScan, BoundedScan, ScanItem, UnboundedScan }
import org.hdfgroup.spark.hdf5.reader.{ DatasetReader, HDF5Reader }
import org.hdfgroup.spark.hdf5.reader.HDF5Schema.{ ArrayVar, GenericNode, HDF5Node }
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

object ScanExecutor {
  sealed trait ScanItem {
    val dataset: ArrayVar[_]
    val ioSize: Int
  }
  case class UnboundedScan(dataset: ArrayVar[_], ioSize: Int) extends ScanItem
  case class BoundedScan(dataset: ArrayVar[_], ioSize: Int, blockNumber: Long = 0) extends ScanItem
  case class BoundedMDScan(dataset: ArrayVar[_], ioSize: Int, blockIndex: Array[Long]) extends ScanItem
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

      case BoundedMDScan(dataset, ioSize, blockIndex) =>
        dataset.dimension.length match {
          case 2 => {
            // Calculations to correctly map the index of each datapoint in
            // respect to the overall linearized matrix.
            val dx = dataset.dimension(0)
            val dy = dataset.dimension(1)
            val x = ioSize * dx / dy
            val y = ioSize * dy / dx
            val blockSizeX = math.sqrt(x).toInt
            val blockSizeY = math.sqrt(y).toInt
            val blockSize = Array[Int](blockSizeX, blockSizeY)
            val yindex = blockSizeY * blockIndex(1)
            val edgeBlockY =
              if (blockIndex(1) <
                ((Math.ceil(dy / math.sqrt(ioSize * dy / dx))).toInt - 1))
                blockSizeY
              else
                dy % yindex
            val dataReader = newDatasetReader(dataset)(_.readDataset(
              blockSize, blockIndex))
            val blockFill = blockIndex(0) * blockSizeX * dy
            dataReader.zipWithIndex.map {
              case (x, index) =>
                Row(
                  fileID,
                  blockFill + (index - index % edgeBlockY) / edgeBlockY * dy +
                    index % edgeBlockY + yindex, x
                )
            }
          }

          case _ => throw new SparkException("Unsupported dataset rank!")
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
