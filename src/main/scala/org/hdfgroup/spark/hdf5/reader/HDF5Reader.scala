package org.hdfgroup.spark.hdf5.reader

import java.io.{ Closeable, File }

import scala.collection.JavaConverters._

import ch.systemsx.cisd.hdf5.{ HDF5DataClass, HDF5DataTypeInformation, HDF5FactoryProvider }
import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.slf4j.LoggerFactory

class HDF5Reader(val input: File, val id: Integer) extends Closeable with Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  val reader = HDF5FactoryProvider.get().openForReading(input)

  // lazy val path: String = input.getPath

  lazy val nodes = listMembers()

  lazy val attributes = listAttributes()

  /*
  private lazy val objects = {
    log.trace("objects")

    getSchema.flatten().map {
      case node @ ArrayVar(_, name, _, _, _, _, _, _) => (name, node)
      case node @ Group(_, name, _) => (name, node)
      case node @ GenericNode(_, _) => (node.path, node)
    }.toMap
  }

  def getObject(path: String): Option[HDF5Node] = objects.get(path)
   */

  override def close(): Unit = reader.close()

  // def isDataset(path: String): Boolean = !reader.isGroup(path)

  def getDataset[S, T](dataset: ArrayVar[T])(fun: DatasetReader[T] => S): S =
    fun(new DatasetReader[T](reader, dataset))

  def getDataset1(dataset: String): Option[HDF5Node] = {
    log.trace("{}", Array[AnyRef](dataset))

    if (reader.exists(dataset)) {
      val node = reader.getDataSetInformation(dataset)
      val hdfType = infoToType(dataset, node.getTypeInformation)
      Option(ArrayVar(input.toString, id, dataset, hdfType, node.getDimensions,
        node.getNumberOfElements, dataset, 1, dataset))
    } else {
      None
    }
  }

  def listMembers(name: String = "/"): HDF5Node = {
    log.trace("{}", Array[AnyRef](name))

    reader.isGroup(name) match {
      case true =>
        val children = reader.getGroupMembers(name).asScala
        name match {
          case "/" => Group(null, name, children.map {
            x => listMembers("/" + x)
          })
          case _ => Group(null, name, children.map {
            x => listMembers(name + "/" + x)
          })
        }
      case false =>
        val info = reader.getDataSetInformation(name)
        try {
          val hdfType = infoToType(name, info.getTypeInformation)
          ArrayVar(input.toString, id, name, hdfType, info.getDimensions,
            info.getNumberOfElements, name, 1, name)
        } catch {
          case _: Throwable =>
            log.warn("Unsupported datatype found (listMembers)")
            GenericNode(null, "")
        }
    }
  }

  def listAttributes(name: String = "/"): HDF5Node = {
    log.trace("{}", Array[AnyRef](name))

    val attributeNames = reader.getAttributeNames(name).asScala
    val attrList = attributeNames.map { x =>
      val info = reader.getAttributeInformation(name, x)
      try {
        val hdfType = infoToType(name, info)
        ArrayVar(input.toString, id, name, hdfType,
          info.getDimensions.map { y => y.toLong },
          info.getNumberOfElements, name, 1, x)
      } catch {
        case _: Throwable =>
          log.warn("Unsupported datatype found (listAttributes)")
          GenericNode(null, "")
      }
    }
    reader.isGroup(name) match {
      case true =>
        val children = reader.getGroupMembers(name).asScala
        name match {
          case "/" => Group(null, name, attrList ++ children.map {
            x => listAttributes("/" + x)
          })
          case _ => Group(null, name, attrList ++ children.map {
            x => listAttributes(name + "/" + x)
          })
        }
      case false => Group(null, name, attrList)
    }
  }

  def infoToType(name: String, info: HDF5DataTypeInformation): HDF5Type[_] = {
    log.trace("{} {}", Array[AnyRef](name, info))

    (info.getDataClass, info.isSigned, info.getElementSize) match {
      case (HDF5DataClass.INTEGER, true, 1) => HDF5Schema.Int8(id, name)
      case (HDF5DataClass.INTEGER, false, 1) => HDF5Schema.UInt8(id, name)
      case (HDF5DataClass.INTEGER, true, 2) => HDF5Schema.Int16(id, name)
      case (HDF5DataClass.INTEGER, false, 2) => HDF5Schema.UInt16(id, name)
      case (HDF5DataClass.INTEGER, true, 4) => HDF5Schema.Int32(id, name)
      case (HDF5DataClass.INTEGER, false, 4) => HDF5Schema.UInt32(id, name)
      case (HDF5DataClass.INTEGER, true, 8) => HDF5Schema.Int64(id, name)
      case (HDF5DataClass.FLOAT, true, 4) => HDF5Schema.Float32(id, name)
      case (HDF5DataClass.FLOAT, true, 8) => HDF5Schema.Float64(id, name)
      case (HDF5DataClass.STRING, signed, size) => HDF5Schema.FLString(id, name)
      case _ => throw new NotImplementedError("Type not supported")
    }
  }
}
