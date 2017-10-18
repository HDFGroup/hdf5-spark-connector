// Copyright (C) 2017 The HDF Group
// All rights reserved.
//
//  \author Hyo-Kyung Lee (hyoklee@hdfgroup.org)
//  \date October 3, 2017
//  \note added multi-dimensional case under buildScan().
package org.hdfgroup.spark.hdf5

import java.io.File
import java.net.URI

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.hdfgroup.spark.hdf5.ScanExecutor._
import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.hdfgroup.spark.hdf5.reader.{HDF5Reader, HDF5Schema}
import org.slf4j.LoggerFactory

class HDF5Relation(val paths: Array[String], val dataset: String, 
                   val fileExtension: Array[String],
                   val chunkSize: Int, 
                   val start: Array[Long], 
                   val block: Array[Int],
                   val index: Array[Long], 
                   val recursion: Boolean)
    (@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with PrunedScan {
    // with InsertableRelation

  private val log = LoggerFactory.getLogger(getClass)

  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  val fileSystem = FileSystem.get(hadoopConfiguration)

  // Gets an array of the files in the directory (and recursively in the
  // sub-directories if specified)
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

  // Maps the files with arbitrary ID's starting at zero
  private lazy val fileIDs = {
    log.trace("fileIDs")
    files.zipWithIndex.map {
      case (uri, index) => (index, uri.toString)
    }.toMap
  }

  def getFileName(id: Integer): Option[String] = fileIDs.get(id)

  // Checks the "dataset" variable and either returns a virtual table or
  // the data, index, and fileID of the files
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

  // TableScan calls PrunedScan with an empty array signalling all columns
  // are to be returned
  override def buildScan(): RDD[Row] = {
    log.trace("buildScan(): RDD[Row]")

    buildScan(Array[String]())
  }

  // PrunedScan that either calls an UnboundedScan or multiple BoundedScans
  // with the requested columns to get an RDD
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    log.trace("buildScan(): RDD[Row]")

    val hasStart = start(0) != -1
    val hasBlock = block(0) != -1
    val scans = datasets.map { UnboundedScan(_, chunkSize, requiredColumns) }
    val splitScans = scans.flatMap {
      case UnboundedScan(ds, size, cols)
      if (ds.size > size || hasStart || hasBlock) =>
        ds.dimension.length match {
          case 1 => {
            val validBlock = hasBlock && block.length == 1
            val startPoint =
              if (hasStart && start.length == 1) start(0)
              else 0L

            if (validBlock) {
              (0L until Math.ceil(block(0).toFloat / size).toLong).map(x => {
                if ((x+1)*size < block(0))
                    BoundedScan(ds, size, x * size + startPoint, cols)
                else
                    BoundedScan(ds, block(0) % (x.toInt*size),
                                x * size + startPoint, cols)
              })
            } else {
              (0L until Math.ceil(ds.size.toFloat / size).toLong).map(x =>
                BoundedScan(ds, size, x * size + startPoint, cols))
            }
          }
          case 2 => {
            val validBlock = hasBlock && block.length == 2
            val startPoint =
              if (hasStart && start.length == 2)
                  start
              else
                  Array(0L, 0L)
            val d =
              if (validBlock)
                  block
              else
                  ds.dimension.map(_.toInt)
            val blockSizeX = math.sqrt(size * d(0) / d(1))
            val blockSizeY = math.sqrt(size * d(1) / d(0))
            val blockSize = Array[Int](blockSizeX.toInt, blockSizeY.toInt)
            // Creates block dimensions roughly proportional to the
            // matrix's dimensions and roughly equivalent to the window size.
            val matrixX = (Math.ceil(d(0) / blockSizeX)).toInt
            val matrixY = (Math.ceil(d(1) / blockSizeY)).toInt

            if (validBlock && block(0)*block(1) <= size) {
              Seq(BoundedMDScan(ds, 0, block, startPoint, cols))
            } else if (validBlock) {
              (0 until (matrixX * matrixY)).map(x => {
              BoundedMDScan(ds, 0, Array[Int](
                  if ((x % matrixX + 1) * blockSize(0) > block(0))
                      (block(0) % (x % matrixX * blockSize(0)))
                  else
                      blockSize(0) ,
                  if ((x / matrixY + 1) * blockSize(1) > block(1))
                      (block(1) % (x / matrixY * blockSize(1)))
                  else
                      blockSize(1)
                ), Array[Long]((x % matrixX * blockSize(0)).toLong
                  + startPoint(0), 
                               (x / matrixX * blockSize(1)).toLong 
                               + startPoint(1)), 
                   cols)
                  }
                ) // map
            } else {
              // Creates bounded scans on the blocks with their corresponding
              // indices to cover the entire matrix
              (0L until (matrixX * matrixY).toLong).map(x =>
              BoundedMDScan(ds,  0, blockSize,
                Array[Long]((x % matrixX * blockSize(0)).toLong 
                            + startPoint(0),
                            (x / matrixX * blockSize(1)).toLong 
                            + startPoint(1)), 
                            cols))
            } // else
          }
          case _ => {
              if (index.length == ds.dimension.length)
                  (0L until 1).map(x =>SlicedMDScan(ds, 0, block, start,
                                                    index, cols))
              else
                  throw new SparkException("Unsupported dataset rank!")
           }
        }
      case x: UnboundedScan => Seq(x)
    }
    sqlContext.sparkContext.parallelize(splitScans).flatMap { item =>
      new ScanExecutor(item.dataset.fileName, item.dataset.fileID).execQuery(item)
    }
  }
}
