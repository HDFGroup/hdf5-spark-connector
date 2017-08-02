package org.hdfgroup.spark.hdf5

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.slf4j.LoggerFactory

class DefaultSource extends RelationProvider {

  private val log = LoggerFactory.getLogger(getClass)

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): BaseRelation = {
    log.trace("{} {}", Array[AnyRef](sqlContext, parameters))

    val paths = parameters.get("path") match {
      case Some(x) => x.split(",").map(_.trim)
      case None => sys.error("'path' must be specified.")
    }

    val dataset = parameters.get("dataset") match {
      case Some(x) => x
      case None => throw new SparkException("You must provide a path to the dataset")
    }

    // Five options that users can specify for reads
    val extensions = parameters.getOrElse("extension", "h5").split(",").map(_.trim)
    val chunkSize = parameters.getOrElse("window size", "10000").toInt
    val start = parameters.getOrElse("start", "-1").split(",").map(_.toLong)
    val block = parameters.getOrElse("block", "-1").split(",").map(_.toInt)
    val recursion = parameters.getOrElse("recursion", "true").toBoolean
    new HDF5Relation(paths, dataset, extensions, chunkSize, start, block, recursion)(sqlContext)
  }
}

class DefaultSource15 extends DefaultSource with DataSourceRegister {

  /* Extension of spark.hdf5.DefaultSource (which is Spark 1.3 and 1.4 compatible) for Spark 1.5.
   * Since the class is loaded through META-INF/services we can decouple the two to have
   * Spark 1.5 byte-code loaded lazily.
   *
   * This trick is adapted from spark elasticsearch-hadoop data source:
   * <https://github.com/elastic/elasticsearch-hadoop>
   */
  override def shortName(): String = "hdf5"
}
