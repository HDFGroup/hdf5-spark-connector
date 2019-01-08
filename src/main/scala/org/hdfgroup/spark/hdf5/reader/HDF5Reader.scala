// Copyright (C) 2017 The HDF Group
// All rights reserved.
//
//  \author Hyo-Kyung Lee (hyoklee@hdfgroup.org)
//
//  \date December 4, 2017
//  \note added array checker.
//
//  \date October 18, 2017
//  \note cleaned up codes.

package org.hdfgroup.spark.hdf5.reader

import java.io.{Closeable, File}
import ch.systemsx.cisd.hdf5.{HDF5DataClass, HDF5DataTypeInformation}
import ch.systemsx.cisd.hdf5.{HDF5FactoryProvider, IHDF5StringReader}
import org.hdfgroup.spark.hdf5.reader.HDF5Schema._
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class HDF5Reader(val input: File, val id: Integer) extends Closeable
    with Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  val reader = HDF5FactoryProvider.get().openForReading(input)

  lazy val nodes = listMembers()

  lazy val attributes = listAttributes()

  override def close(): Unit = reader.close()

  def getDataset[S, T](dataset: ArrayVar[T])(fun: DatasetReader[T] => S):
      S = fun(new DatasetReader[T](reader, dataset))

  // TODO Check if we can push down the dataset and attribute traversals
  //      into the scans (for parallelism)

  // Returns an ArrayVar if the specified dataset exists
  def getDataset1(dataset: String): Option[HDF5Node] = {
    log.trace("{}", Array[AnyRef](dataset))

    dataset match {
      case "sparky://files" =>
        Option(
          ArrayVar(input.toString, id, dataset, HDF5Schema.FLString(id, dataset),
            Array(1), 1, input.toString, input.length, input.toString, ""))
      case "sparky://datasets" =>
        Option(
          Catalog(id, input.toString,
            {
              val reader = new HDF5Reader(input, id)
              val nodes = reader.nodes.flatten()
              reader.close()
              nodes.collect{
                case y: ArrayVar[_] => new ArrayVar(input.toString, id, dataset,
                  y.contains, y.dimension, 1, y.path, y.size, null, "")
              }
            }
          )
        )
      case "sparky://attributes" =>
        Option(
          Catalog(id, input.toString,
            {
              val reader = new HDF5Reader(input, id)
              val nodes = reader.attributes.flatten()
              reader.close()
              nodes.collect{
                case y: ArrayVar[_] => new ArrayVar(input.toString, id, dataset,
                  y.contains, y.dimension, 1, y.path, y.size, y.attribute,
                  y.value)
              }
            }
          )
        )
      case _ =>
        if (reader.exists(dataset)) {
          val node = reader.getDataSetInformation(dataset)
          val hdfType = infoToType(dataset, node.getTypeInformation)
          Option(
            ArrayVar(
              input.toString,
              id,
              dataset,
              hdfType,
              node.getDimensions,
              node.getNumberOfElements,
              dataset,
              1,
              dataset,
              ""))
        }
        else {
          None
        }
    }
  }

  def listMembers(name: String = "/"): HDF5Node = {
    log.trace("{}", Array[AnyRef](name))

    reader.isGroup(name) match {
      case true =>
        val children = reader.getGroupMembers(name).asScala
        name match {
          case "/" =>
            Group(
              null,
              name,
              children.map { x => listMembers("/" + x) })
          case _ =>
            Group(
              null, name, children.map { x => listMembers(name + "/" + x) })
        }
      case false =>
        val info = reader.getDataSetInformation(name)
        try {
          val hdfType = infoToType(name, info.getTypeInformation)
          ArrayVar(
            input.toString,
            id,
            name,
            hdfType,
            info.getDimensions,
            info.getNumberOfElements,
            name,
            1,
            name,
            "")
        } catch {
          case _: Throwable =>
            log.warn("Unsupported datatype found (listMembers)")
            GenericNode(null, "")
        }
    }
  }

  def listAttributes(name: String = "/"): HDF5Node = {
    log.trace("{}", Array[AnyRef](name))

    val attributeNames = reader.`object`().getAttributeNames(name).asScala

    val attrList = attributeNames.map {
      x => val info = reader.`object`().getAttributeInformation(name, x)

      try {
        val hdfType = infoToType(name, info)
        val attr = getAttributeValueAsString(name, x, hdfType)
        // println(attr)
        ArrayVar(input.toString, id, name, hdfType,
          info.getDimensions.map { y => y.toLong },
          info.getNumberOfElements, name, 1, x, attr)
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
      case (HDF5DataClass.INTEGER, false, 8) => HDF5Schema.UInt64(id, name)
      case (HDF5DataClass.FLOAT, true, 4) => HDF5Schema.Float32(id, name)
      case (HDF5DataClass.FLOAT, true, 8) => HDF5Schema.Float64(id, name)
      case (HDF5DataClass.STRING, signed, size) => HDF5Schema.FLString(id, name)
      case _ => throw new NotImplementedError("Type not supported")
    }
  }

  def getAttributeValueAsString(
    name: String,
    x: String,
    hdfType: HDF5Type[_]): String = {
    var attr = ""
    // println("getAttributeValueAsString()="+name+":"+x)
    val info = reader.`object`().getAttributeInformation(name, x)
    val a = info.getDimensions()

    if (a.size > 0) {
      val v = hdfType match {
        case Int8(_,_) => reader.int8().getArrayAttr(name, x)
        case UInt8(_,_) => reader.int16().getArrayAttr(name, x)
        case Int16(_,_) => reader.int16().getArrayAttr(name, x)
        case UInt16(_,_) => reader.int32().getArrayAttr(name, x)
        case Int32(_,_) => reader.int32().getArrayAttr(name, x)
        case UInt32(_,_) => reader.int64().getArrayAttr(name, x)
        case Int64(_,_) => reader.int64().getArrayAttr(name, x)
        case UInt64(_,_) => reader.float64().getArrayAttr(name, x)
        case Float32(_,_) => reader.float32().getArrayAttr(name, x)
        case Float64(_,_) => reader.float64().getArrayAttr(name, x)
        case FLString(_,_) => reader.string().getArrayAttr(name, x)
        case _ => Array("UNKNOWN")
      }
      var buf = ""
      for(z <- v) {
        buf += z.toString
        buf += ","
      }
      attr = buf.dropRight(1)
    }
    else {
      val f = hdfType match {
        case Int8(_,_) => reader.int8().getAttr(name, x)
        case UInt8(_,_) => reader.int16().getAttr(name, x)
        case Int16(_,_) => reader.int16().getAttr(name, x)
        case UInt16(_,_) => reader.int32().getAttr(name, x)
        case Int32(_,_) => reader.int32().getAttr(name, x)
        case UInt32(_,_) => reader.int64().getAttr(name, x)
        case Int64(_,_) => reader.int64().getAttr(name, x)
        case UInt64(_,_) => reader.float64().getAttr(name, x)
        case Float32(_,_) => reader.float32().getAttr(name, x)
        case Float64(_,_) => reader.float64().getAttr(name, x)
        case FLString(_,_) => reader.string().getAttr(name, x)
        case _ => "UNKNOWN"
      }
      attr += f.toString
    }

    attr
  }
}
