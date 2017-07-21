package org.hdfgroup.spark.hdf5

import java.io.File
import java.net.URI

import org.hdfgroup.spark.hdf5.ScanExecutor.{ BoundedMDScan, BoundedScan, UnboundedScan }
import org.hdfgroup.spark.hdf5.reader.{ HDF5Reader, HDF5Schema }
import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class HDF5Relation(val paths: Array[String], val dataset: String, val fileExtension: Array[String],
                   val chunkSize: Int, val start: Array[Long], val block: Array[Long])(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  private val log = LoggerFactory.getLogger(getClass)

  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  val fileSystem = FileSystem.get(hadoopConfiguration)

  lazy val files: Array[URI] = {
    log.trace("files: Array[URI]")

    val roots = paths.map { path =>
      fileSystem.getFileStatus(new Path(path))
    }.toSeq

    val leaves = roots.flatMap {
      case status if status.isFile => Set(status)
      case directory if directory.isDirectory =>
        val it = fileSystem.listFiles(directory.getPath, true)
        var children: Set[FileStatus] = Set()
        while (it.hasNext) {
          children += it.next()
        }
        children
    }
    leaves.filter(status => status.isFile)
      .map(_.getPath)
      .filter(path => fileExtension.contains(FilenameUtils.getExtension(path.toString)))
      .map(org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(_).toUri)
      .toArray
  }

  private lazy val fileIDs = {
    log.trace("fileIDs")
    files.zipWithIndex.map {
      case (uri, index) => (index, uri.toString)
    }.toMap
  }

  def getFileName(id: Integer): Option[String] = fileIDs.get(id)

  private lazy val datasets: Array[ArrayVar[_]] = {
    log.trace("datasets: Array[ArrayVar[_]]")

    dataset match {
      case "sparky://files" => fileIDs.map {
        case (id, name) =>
          new ArrayVar(name, id, dataset, HDF5Schema.FLString(id, dataset),
            Array(1), 1, name, new File(name).length, name)
      }.toArray

      case "sparky://datasets" => fileIDs.map {
        case (id, name) =>
        {
          val reader = new HDF5Reader(new File(name), id)
          val nodes = reader.nodes.flatten()
          reader.close( )
          nodes collect {
            case y: ArrayVar[_] => new ArrayVar(name, id, dataset,
              y.contains, y.dimension, 1, y.path, y.size, null)
          }
        }
      }.toArray.flatten

      case "sparky://attributes" => fileIDs.map {
        case (id, name) =>
        {
          val reader = new HDF5Reader(new File(name), id)
          val nodes = reader.attributes.flatten()
          reader.close()
          nodes collect {
            case y: ArrayVar[_] => new ArrayVar(name, id, dataset,
              y.contains, y.dimension, 1, y.path, y.size, y.attribute)
          }
        }
      }.toArray.flatten

      case _ => fileIDs.flatMap {
        case (id, name) =>
          new ScanExecutor(name, id).openReader(_.getDataset1(dataset))
      }.toArray.collect { case y: ArrayVar[_] => y }
    }
  }

  private lazy val hdf5Schema: ArrayVar[_] = {
    log.trace("hdf5Schema: ArrayVar[_]")

    datasets match {
      case Array(head: ArrayVar[_], _*) => head
      case _ => throw new java.io.FileNotFoundException("No files")
    }
  }

  override def schema: StructType = SchemaConverter.convertSchema(hdf5Schema)

  override def buildScan(): RDD[Row] = {
    log.trace("buildScan(): RDD[Row]")

    val hasStart = start(0) != -1
    val hasBlock = block(0) != -1

    val scans = datasets.map { UnboundedScan(_, chunkSize) }
    val splitScans = scans.flatMap {
      case UnboundedScan(ds, size) if (ds.size > size || hasStart || hasBlock) =>
        ds.dimension.length match {
          case 1 => {
            val startPoint =
              if (start(0) == -1) 0L
              else start(0)
            if (block(0) == -1) {
              (0L until Math.ceil(ds.size.toFloat / size).toLong).map(x =>
                BoundedScan(ds, size, x * size + startPoint))
            } else {
                Seq(BoundedScan(ds, block(0).toInt, block(0) + startPoint))
            }
          }
          case 2 => {
            val startPoint =
              if (start(0) == -1) Array(0, 0)
              else start
            val d =
              if (block(0) == -1) ds.dimension
              else block
            // Creates block dimensions roughly proportional to the
            val matrixX = (Math.ceil(d(0) / math.sqrt(size * d(0) / d(1)))).toInt
            // matrix's dimensions and roughly equivalent to the window size.
            val matrixY = (Math.ceil(d(1) / math.sqrt(size * d(1) / d(0)))).toInt
            // Creates bounded scans on the blocks with their corresponding
            // indices to cover the entire matrix
            (0L until (matrixX * matrixY).toLong).map(i => BoundedMDScan(ds, size, Array[Long]((i % matrixX).toLong, (i / matrixX).toLong)))
          }

          case _ => throw new SparkException("Unsupported dataset rank!")
        }
      case x: UnboundedScan => Seq(x)
    }
    sqlContext.sparkContext.parallelize(splitScans).flatMap { item =>
      new ScanExecutor(item.dataset.fileName, item.dataset.fileID).execQuery(item)
    }
  }
}
