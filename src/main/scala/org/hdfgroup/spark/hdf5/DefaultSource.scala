package org.hdfgroup.spark.hdf5

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.slf4j.LoggerFactory

class DefaultSource extends RelationProvider {

  private val log = LoggerFactory.getLogger(getClass)

  // TODO What's the correct way to fail if pre-conditions aren't met?

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    log.trace("{} {}", Array[AnyRef](sqlContext, parameters))

    // The name of a text file containing a list of file names (one per line)
    val flist = parameters.get("files") match {
      case Some(x) => x.trim
      case None => ""
    }

    // A comma-separated list of file and directory paths (or empty)
    val paths = parameters.get("path") match {
      case Some(x) => {
        val a = x.trim.split(",").map(_.trim)
        if (a.size == 1 && a(0) == "") Array[String]() else a
      }
      case None => Array[String]()
    }

    if (flist == "" && paths.size == 0) {
      sys.error("'files' or 'path' must be specified.")
    }

    // The HDF5 path name of the dataset of interest
    val dataset = parameters.get("dataset") match {
      case Some(x) => x
      case None =>
        throw new SparkException("A dataset path must be specified")
    }

    // Options

    // The HDF5 file extensions to probe.
    val extensions =
      parameters.getOrElse("extension", "h5").split(",").map(_.trim)

    // The I/O window size in NUMBER OF ELEMENTS.
    val chunkSize = parameters.getOrElse("window size", "10000").toInt

    // Hyperslab

    // TODO Add support for strides and counts
    // TODO Check the consistency of the spec
    // TODO Define a more compact representation

    val start = parameters.getOrElse("start", "-1").split(",").map(_.toLong)
    val block = parameters.getOrElse("block", "-1").split(",").map(_.toInt)
    val stride = parameters.getOrElse("stride", "-1").split(",").map(_.toInt)
    val count = parameters.getOrElse("count", "-1").split(",").map(_.toInt)

    // Block index (???)
    val index = parameters.getOrElse("index", "-1").split(",").map(_.toLong)

    // The recursion behavior for directories
    val recursion = parameters.getOrElse("recursion", "true").toBoolean

    new HDF5Relation(
      flist,
      paths,
      dataset,
      extensions,
      chunkSize,
      start,
      block,
      index,
      recursion)(sqlContext)
  }
}

class DefaultSource15 extends DefaultSource with DataSourceRegister {

  /* Extension of spark.hdf5.DefaultSource (which is Spark 1.3 and 1.4
   * compatible) for Spark 1.5. Since the class is loaded through
   * META-INF/services we can decouple the two to have * Spark 1.5 byte-code
   * loaded lazily. This trick is adapted from spark elasticsearch-hadoop data
   * source: <https://github.com/elastic/elasticsearch-hadoop>
   */
  override def shortName(): String = "hdf5"
}
