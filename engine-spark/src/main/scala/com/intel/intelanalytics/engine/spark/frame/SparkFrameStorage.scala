//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.schema.{Schema, DataTypes}
import DataTypes.DataType
import java.nio.file.Paths
import scala.io.{Source, Codec}
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.{HdfsFileStorage, SparkOps}
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import java.util.concurrent.atomic.AtomicLong
import com.intel.intelanalytics.domain.frame.{Column, DataFrame, DataFrameTemplate}
import com.intel.intelanalytics.engine.spark.context.{SparkContextManager}
import scala.Some
import com.intel.intelanalytics.engine.File
import com.intel.intelanalytics.security.UserPrincipal
import org.joda.time.DateTime

class SparkFrameStorage(contextManager: SparkContextManager, fsRoot: String, files: HdfsFileStorage)
  extends FrameStorage with EventLogging {

  import spray.json._
  import Rows.Row

  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  def updateName(frame: DataFrame, newName: String): DataFrame = {
    val newFrame = frame.copy(name = newName)
    updateMeta(newFrame)
  }

  def updateSchema(frame: DataFrame, columns: List[(String, DataType)]): DataFrame = {
    val newSchema = frame.schema.copy(columns = columns)
    val newFrame = frame.copy(schema = newSchema)
    updateMeta(newFrame)
  }

  private def updateMeta(newFrame: DataFrame): DataFrame = {
    val meta = File(Paths.get(getFrameMetaDataFile(newFrame.id)))
    info(s"Saving metadata to $meta")
    val f = files.write(meta)
    try {
      val json: String = newFrame.toJson.prettyPrint
      debug(json)
      f.write(json.getBytes(Codec.UTF8.name))
    }
    finally {
      f.close()
    }
    newFrame
  }

  override def drop(frame: DataFrame): Unit = withContext("frame.drop") {
    files.delete(Paths.get(getFrameDirectory(frame.id)))
  }

  override def appendRows(startWith: DataFrame, append: Iterable[Row]): Unit =
    withContext("frame.appendRows") {
      ???
    }

  override def removeRows(frame: DataFrame, predicate: (Row) => Boolean): Unit =
    withContext("frame.removeRows") {
      ???
    }

  override def removeColumn(frame: DataFrame, columnIndex: Seq[Int]): Unit =
    withContext("frame.removeColumn") {

      val remainingColumns = {
        columnIndex match {
          case singleColumn if singleColumn.length == 1 =>
            frame.schema.columns.take(singleColumn(0)) ++ frame.schema.columns.drop(singleColumn(0) + 1)
          case _ =>
            frame.schema.columns.zipWithIndex.filter(elem => columnIndex.contains(elem._2) == false).map(_._1)
        }
      }
      updateSchema(frame, remainingColumns)
    }

  override def addColumnWithValue[T](frame: DataFrame, column: Column[T], default: T): Unit =
    withContext("frame.addColumnWithValue") {
      ???
    }

  override def renameFrame(frame: DataFrame, newName: String): Unit =
    withContext("frame.rename") {
      updateName(frame, newName)
    }

  override def renameColumn(frame: DataFrame, name_pairs: Seq[(String, String)]): Unit =
    withContext("frame.renameColumn") {
      val columnsToRename: Seq[String] = name_pairs.map(_._1)
      val newColumnNames: Seq[String] = name_pairs.map(_._2)

      def generateNewColumnTuple(oldColumn: String, columnsToRename: Seq[String], newColumnNames: Seq[String]): String = {
        val result = columnsToRename.indexOf(oldColumn) match {
          case notFound if notFound < 0 => oldColumn
          case found => newColumnNames(found)
        }
        result
      }

      val newColumns = frame.schema.columns.map(col => (generateNewColumnTuple(col._1, columnsToRename, newColumnNames), col._2))
      updateSchema(frame, newColumns)
    }

  override def addColumn[T](frame: DataFrame, column: Column[T], columnType: DataTypes.DataType): DataFrame =
    withContext("frame.addColumn") {
      val newColumns = frame.schema.columns :+ (column.name, columnType)
      updateSchema(frame, newColumns)
    }

  override def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row] =
    withContext("frame.getRows") {
      require(frame != null, "frame is required")
      require(offset >= 0, "offset must be zero or greater")
      require(count > 0, "count must be zero or greater")
      val ctx = contextManager.getContext("intelanalytics:" + user.user.username).sparkContext
      val rdd: RDD[Row] = getFrameRdd(ctx, frame.id)
      val rows = SparkOps.getRows(rdd, offset, count)
      rows
    }

  /**
   * Create an RDD from a frame data file.
   * @param ctx spark context
   * @param frameId primary key of the frame record
   * @return the newly created RDD
   */
  def getFrameRdd(ctx: SparkContext, frameId: Long): RDD[Row] = {
    ctx.objectFile[Row](fsRoot + getFrameDataFile(frameId))
  }

  def getOrCreateDirectory(name: String): Directory = {
    val path = Paths.get(name)
    val meta = files.getMetaData(path).getOrElse(files.createDirectory(path))
    meta match {
      case File(f, s) => throw new IllegalArgumentException(path + " is not a directory")
      case d: Directory => d
    }
  }

  override def create(frame: DataFrameTemplate): DataFrame = withContext("frame.create") {
    val id = nextFrameId()
    // TODO: wire this up better.  For example, status Id should be looked up, uri needs to be supplied, user supplied, etc.
    val frame2 = new DataFrame(id = id, name = frame.name, description = frame.description, uri = "TODO", schema = Schema(), status = 1L, new DateTime(), new DateTime(), None, None)
    val meta = File(Paths.get(getFrameMetaDataFile(id)))
    info(s"Saving metadata to $meta")
    val f = files.write(meta)
    try {
      val json: String = frame2.toJson.prettyPrint
      debug(json)
      f.write(json.getBytes(Codec.UTF8.name))
    }
    finally {
      f.close()
    }
    frame2
  }

  override def lookup(id: Long): Option[DataFrame] = withContext("frame.lookup") {
    val path = getFrameDirectory(id)
    val meta = File(Paths.get(path, "meta"))
    if (files.getMetaData(meta.path).isEmpty) {
      return None
    }
    val f = files.read(meta)
    try {
      val src = Source.fromInputStream(f)(Codec.UTF8).getLines().mkString("")
      val json = JsonParser(src)
      return Some(json.convertTo[DataFrame])
    }
    finally {
      f.close()
    }
  }

  val idRegex: Regex = "^\\d+$".r

  def getFrames(offset: Int, count: Int): Seq[DataFrame] = withContext("frame.getFrames") {
    files.list(getOrCreateDirectory(frameBase))
      .flatMap {
      case Directory(p) => Some(p.getName(p.getNameCount - 1).toString)
      case _ => None
    }
      .filter(idRegex.findFirstMatchIn(_).isDefined) //may want to extract a method for this
      .flatMap(sid => lookup(sid.toLong))
  }

  val frameBase = "/intelanalytics/dataframes"
  //temporary
  var frameId = new AtomicLong(1)

  def nextFrameId() = {
    //Just a temporary implementation, only appropriate for scaffolding.
    frameId.getAndIncrement
  }

  def getFrameDirectory(id: Long): String = {
    val path = Paths.get(s"$frameBase/$id")
    path.toString
  }

  def getFrameDataFile(id: Long): String = {
    getFrameDirectory(id) + "/data"
  }

  def getFrameMetaDataFile(id: Long): String = {
    getFrameDirectory(id) + "/meta"
  }
}
