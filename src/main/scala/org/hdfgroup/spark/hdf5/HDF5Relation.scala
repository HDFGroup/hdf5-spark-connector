package org.hdfgroup.spark.hdf5

import java.io.File
import java.net.URI

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{ BaseRelation, PrunedScan, TableScan }
import org.apache.spark.sql.types.StructType
import org.hdfgroup.spark.hdf5.ScanExecutor._
import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.hdfgroup.spark.hdf5.reader.{ HDF5Reader, HDF5Schema }
import org.slf4j.LoggerFactory

class HDF5Relation(val paths: Array[String], val dataset: String, val fileExtension: Array[String],
                   val chunkSize: Int, val start: Array[Long], val block: Array[Int],
                   val recursion: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan {
  // with InsertableRelation

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
        val it = fileSystem.listFiles(directory.getPath, recursion)
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
        case (id, name) => {
          val reader = new HDF5Reader(new File(name), id)
          val nodes = reader.nodes.flatten()
          reader.close()
          nodes collect {
            case y: ArrayVar[_] => new ArrayVar(name, id, dataset,
              y.contains, y.dimension, 1, y.path, y.size, null)
          }
        }
      }.toArray.flatten

      case "sparky://attributes" => fileIDs.map {
        case (id, name) => {
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

    buildScan(Array[String]())
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    log.trace("buildScan(): RDD[Row]")

    val hasStart = start(0) != -1
    val hasBlock = block(0) != -1

    val scans = datasets.map { UnboundedScan(_, chunkSize, requiredColumns) }
    val splitScans = scans.flatMap {
      case UnboundedScan(ds, size, cols) if (ds.size > size || hasStart || hasBlock) =>
        ds.dimension.length match {
          case 1 => {
            val startPoint =
              if (hasStart && start.length == 1) start(0)
              else 0L

            if (hasBlock && block.length == 1 && block(0) <= size) {
              Seq(BoundedScan(ds, block(0), startPoint, cols))
            } else if (hasBlock && block.length == 1) {
              (0L until Math.ceil(block(0).toFloat / size).toLong).map(x => {
                if ((x+1)*size < block(0)) BoundedScan(ds, size, x * size + startPoint, cols)
                else BoundedScan(ds, block(0) % (x.toInt*size), x * size + startPoint, cols)
              })
            } else {
              (0L until Math.ceil(ds.size.toFloat / size).toLong).map(x =>
                BoundedScan(ds, size, x * size + startPoint, cols))
            }
          }
          case 2 => {
            val startPoint =
              if (hasStart && start.length == 2) start
              else Array(0L, 0L)

            if (hasBlock && block.length == 2 && block(0)*block(1) <= size) {
              Seq(BoundedMDScan(ds, 0, block, startPoint, cols))
            } else if (hasBlock && block.length == 2) {
              val blockSizeX = math.sqrt(size * block(0) / block(1))
              val blockSizeY = math.sqrt(size * block(1) / block(0))
              val blockSize = Array[Int](blockSizeX.toInt, blockSizeY.toInt)
              val matrixX = (Math.ceil(block(0) / blockSizeX)).toInt
              val matrixY = (Math.ceil(block(1) / blockSizeY)).toInt
              (0 until (matrixX * matrixY)).map(x => {
                BoundedMDScan(ds, 0, Array[Int](
                  if ((x % matrixX + 1) * blockSize(0) > block(0)) {
                    (block(0) % (x % matrixX * blockSize(0)))
                  } else blockSize(0) ,
                  if ((x / matrixY + 1) * blockSize(1) > block(1)){
                    (block(1) % (x / matrixY * blockSize(1)))
                  } else blockSize(1)
                ), Array[Long]((x % matrixX * blockSize(0)).toLong
                  + startPoint(0), (x / matrixX * blockSize(1)).toLong + startPoint(1)), cols)
              })
            }
            else {
              val d = ds.dimension
              val blockSizeX = math.sqrt(size * d(0) / d(1))
              val blockSizeY = math.sqrt(size * d(1) / d(0))
              val blockSize = Array[Int](blockSizeX.toInt, blockSizeY.toInt)
              // Creates block dimensions roughly proportional to the
              // matrix's dimensions and roughly equivalent to the window size.
              val matrixX = (Math.ceil(d(0) / blockSizeX)).toInt
              val matrixY = (Math.ceil(d(1) / blockSizeY)).toInt
              // Creates bounded scans on the blocks with their corresponding
              // indices to cover the entire matrix
              (0L until (matrixX * matrixY).toLong).map(x => BoundedMDScan(ds, 0, blockSize, Array[Long]((x % matrixX *
                blockSize(0)).toLong + startPoint(0), (x / matrixX * blockSize(1)).toLong + startPoint(1)), cols))

            }
          }

          case _ => throw new SparkException("Unsupported dataset rank!")
        }
      case x: UnboundedScan => Seq(x)
    }
    sqlContext.sparkContext.parallelize(splitScans).flatMap { item =>
      new ScanExecutor(item.dataset.fileName, item.dataset.fileID).execQuery(item)
    }
  }
  /*
  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    if (overwrite) {
      try {
        fileSystem.delete(filesystem, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${FileSystem.toString} prior"
              + s" to INSERT OVERWRITE a HDF5 table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.

      // ADD A HDF5 SAVE FILE METHOD
      // XmlFile.saveAsXmlFile(data, filesystemPath.toString, parameters)
      // val codecClass = CompressionCodecs.getCodecClass(codec)
      // data.saveAsCsvFile(filesystemPath.toString, Map("delimiter" -> delimiter.toString),
      // codecClass)
    } else {
      sys.error("HDF5 tables only support INSERT OVERWRITE for now.")
    }
  }*/
}
