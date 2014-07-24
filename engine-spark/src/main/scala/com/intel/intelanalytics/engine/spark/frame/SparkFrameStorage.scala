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

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import DataTypes.DataType
import java.nio.file.Paths
import com.intel.intelanalytics.shared.EventLogging

import scala.io.{ Codec, Source }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.{ SparkEngineConfig, HdfsFileStorage, SparkOps, SparkComponent }
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import java.util.concurrent.atomic.AtomicLong
import com.intel.intelanalytics.domain.frame.{ Column, DataFrame, DataFrameTemplate }
import com.intel.intelanalytics.engine.spark.context.{ Context }
import com.intel.intelanalytics.engine.File
import com.intel.intelanalytics.security.UserPrincipal
import org.joda.time.DateTime
import com.intel.intelanalytics.engine.{ FrameStorage, FrameComponent }
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent, MetaStore, MetaStoreComponent }

class SparkFrameStorage(context: UserPrincipal => Context, fsRoot: String, files: HdfsFileStorage, maxRows: Int, val metaStore: SlickMetaStoreComponent#SlickMetaStore)
    extends FrameStorage with EventLogging with ClassLoaderAware {

  import spray.json._
  import Rows.Row

  def updateSchema(frame: DataFrame, columns: List[(String, DataType)]): DataFrame = {
    metaStore.withSession("frame.updateSchema") {
      implicit session =>
        {
          metaStore.frameRepo.updateSchema(frame, columns)
        }
    }
  }

  def updateRowCount(frame: DataFrame, rowCount: Long): DataFrame = {
    metaStore.withSession("frame.updateCount") {
      implicit session =>
        {
          metaStore.frameRepo.updateRowCount(frame, rowCount)
        }
    }
  }

  override def drop(frame: DataFrame): Unit = {
    deleteFrameFile(frame.id)
    metaStore.withSession("frame.drop") {
      implicit session =>
        {
          metaStore.frameRepo.delete(frame.id)
          Unit

        }
    }
  }

  /**
   * Remove the underlying data file from HDFS.
   * @param frameId primary key from Frame table
   */
  private def deleteFrameFile(frameId: Long): Unit = {
    files.delete(Paths.get(getFrameDirectory(frameId)))
  }

  override def removeColumn(frame: DataFrame, columnIndex: Seq[Int])(implicit user: UserPrincipal): DataFrame =
    //withContext("frame.removeColumn") {
    metaStore.withSession("frame.removeColumn") {
      implicit session =>
        {
          val remainingColumns = {
            columnIndex match {
              case singleColumn if singleColumn.length == 1 =>
                frame.schema.columns.take(singleColumn(0)) ++ frame.schema.columns.drop(singleColumn(0) + 1)
              case _ =>
                frame.schema.columns.zipWithIndex.filter(elem => !columnIndex.contains(elem._2)).map(_._1)
            }
          }
          metaStore.frameRepo.updateSchema(frame, remainingColumns)
        }
    }

  override def renameFrame(frame: DataFrame, newName: String): DataFrame = {
    metaStore.withSession("frame.rename") {
      implicit session =>
        {
          val newFrame = frame.copy(name = newName)
          metaStore.frameRepo.update(newFrame).get
        }
    }
  }
  override def renameColumns(frame: DataFrame, name_pairs: Seq[(String, String)]): DataFrame =
    //withContext("frame.renameColumns") {
    metaStore.withSession("frame.renameColumns") {
      implicit session =>
        {
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
          metaStore.frameRepo.updateSchema(frame, newColumns)

        }
    }

  override def addColumn[T](frame: DataFrame, column: Column[T], columnType: DataTypes.DataType): DataFrame =
    //withContext("frame.addColumn") {
    metaStore.withSession("frame.addColumn") {
      implicit session =>
        {
          val newColumns = frame.schema.columns :+ (column.name, columnType)
          metaStore.frameRepo.updateSchema(frame, newColumns)
        }
    }

  override def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row] =
    withContext("frame.getRows") {
      require(frame != null, "frame is required")
      require(offset >= 0, "offset must be zero or greater")
      require(count > 0, "count must be zero or greater")
      withMyClassLoader {
        val ctx = context(user).sparkContext
        val rdd: RDD[Row] = getFrameRowRdd(ctx, frame.id)
        val rows = SparkOps.getRows(rdd, offset, count, maxRows)
        rows
      }
    }

  /**
   * Create a FrameRDD or throw an exception if bad frameId is given
   * @param ctx spark context
   * @param frameId primary key of the frame record
   * @return the newly created RDD
   */
  def getFrameRdd(ctx: SparkContext, frameId: Long): FrameRDD = {
    val frame = lookup(frameId).getOrElse(
      throw new IllegalArgumentException(s"No such data frame: ${frameId}"))
    getFrameRdd(ctx, frame)
  }

  /**
   * Create an FrameRDD from a frame data file
   * @param ctx spark context
   * @param frame the model for the frame
   * @return the newly created FrameRDD
   */
  def getFrameRdd(ctx: SparkContext, frame: DataFrame): FrameRDD = {
    new FrameRDD(frame.schema, getFrameRowRdd(ctx, frame.id))
  }

  /**
   * Create an RDD from a frame data file.
   * @param ctx spark context
   * @param frameId primary key of the frame record
   * @return the newly created RDD
   */
  def getFrameRowRdd(ctx: SparkContext, frameId: Long): RDD[Row] = {
    val path: String = getFrameDataFile(frameId)
    val absPath = fsRoot + path
    files.getMetaData(Paths.get(path)) match {
      case None => ctx.parallelize(Nil)
      case _ => ctx.objectFile[Row](absPath, SparkEngineConfig.sparkDefaultPartitions)
    }
  }

  override def lookupByName(name: String)(implicit user: UserPrincipal): Option[DataFrame] = {
    metaStore.withSession("frame.lookupByName") {
      implicit session =>
        {
          metaStore.frameRepo.lookupByName(name)
        }
    }
  }

  /**
   * Get the pair of FrameRDD's that were the result of a parse
   * @param ctx spark context
   * @param frame the model of the frame that was the successfully parsed lines
   * @param errorFrame the model for the frame that was the parse errors
   */
  def getParseResult(ctx: SparkContext, frame: DataFrame, errorFrame: DataFrame): ParseResultRddWrapper = {
    val frameRdd = getFrameRdd(ctx, frame)
    val errorFrameRdd = getFrameRdd(ctx, errorFrame)
    new ParseResultRddWrapper(frameRdd, errorFrameRdd)
  }

  def getOrCreateDirectory(name: String): Directory = {
    val path = Paths.get(name)
    val meta = files.getMetaData(path).getOrElse(files.createDirectory(path))
    meta match {
      case File(f, s) => throw new IllegalArgumentException(path + " is not a directory")
      case d: Directory => d
    }
  }

  override def lookup(id: Long): Option[DataFrame] = {
    metaStore.withSession("frame.lookup") {
      implicit session =>
        {
          metaStore.frameRepo.lookup(id)
        }
    }
  }

  override def getFrames(offset: Int, count: Int)(implicit user: UserPrincipal): Seq[DataFrame] = {
    metaStore.withSession("frame.getFrames") {
      implicit session =>
        {
          metaStore.frameRepo.scan(offset, count)
        }
    }
  }

  override def create(frameTemplate: DataFrameTemplate)(implicit user: UserPrincipal): DataFrame = {
    metaStore.withSession("frame.createFrame") {
      implicit session =>
        {
          val frame = metaStore.frameRepo.insert(frameTemplate).get

          //remove any existing artifacts to prevent collisions when a database is reinitialized.
          deleteFrameFile(frame.id)

          frame
        }
    }
  }

  /**
   * Get the error frame of the supplied frame or create one if it doesn't exist
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  override def lookupOrCreateErrorFrame(frame: DataFrame): DataFrame = {
    val errorFrame = lookupErrorFrame(frame)
    if (!errorFrame.isDefined) {
      metaStore.withSession("frame.lookupOrCreateErrorFrame") {
        implicit session =>
          {
            // TODO: remove hard-coded strings
            val errorTemplate = new DataFrameTemplate(frame.name + "-parse-errors", Some("This frame was automatically created to capture parse errors for " + frame.name))
            val newlyCreateErrorFrame = metaStore.frameRepo.insert(errorTemplate).get
            metaStore.frameRepo.updateErrorFrameId(frame, Some(newlyCreateErrorFrame.id))
            newlyCreateErrorFrame
          }
      }
    }
    else {
      errorFrame.get
    }
  }

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  override def lookupErrorFrame(frame: DataFrame): Option[DataFrame] = {
    if (frame.errorFrameId.isDefined) {
      val errorFrame = lookup(frame.errorFrameId.get)
      if (!errorFrame.isDefined) {
        error("Frame referenced an error frame that does NOT exist: " + frame.errorFrameId.get)
      }
      errorFrame
    }
    else {
      None
    }

  }

  val idRegex: Regex = "^\\d+$".r

  val frameBase = "/intelanalytics/dataframes"
  //temporary
  var frameId = new AtomicLong(1)

  //  def nextFrameId() = {
  //    //Just a temporary implementation, only appropriate for scaffolding.
  //    frameId.getAndIncrement
  //  }
  //
  def getFrameDirectory(id: Long): String = {
    val path = Paths.get(s"$frameBase/$id")
    path.toString
  }
  //
  //  def getFrameDirectoryByName(name: String): String = {
  //    //val path = Paths.get(s"something")
  //    val path = Paths.get(s"$frameBase/$name")
  //    path.toString
  //  }
  //

  def getFrameDataFile(id: Long): String = {
    getFrameDirectory(id) + "/data"
  }

  def getFrameMetaDataFile(id: Long): String = {
    getFrameDirectory(id) + "/meta"
  }
  //
  //  def getFrameMetaDataFileByName(name: String): String = {
  //    getFrameDirectoryByName(name) + "/meta"
  //  }
  //
  //  def getFrameDataFileByName(name: String): String = {
  //    getFrameDirectoryByName(name) + "/data"
  //  }
}
