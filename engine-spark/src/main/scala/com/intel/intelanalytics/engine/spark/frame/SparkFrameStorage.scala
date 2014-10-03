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

import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.schema.DataTypes
import DataTypes.DataType
import java.nio.file.Paths
import com.intel.intelanalytics.engine.spark.frame.parquet.ParquetReader

import scala.io.Codec
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark._
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import java.util.concurrent.atomic.AtomicLong
import com.intel.intelanalytics.domain.frame.{ FrameReference, Column, DataFrameTemplate, DataFrame }
import com.intel.intelanalytics.engine.FrameStorage
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent, MetaStoreComponent }
import com.intel.intelanalytics.engine.plugin.Invocation
import scala.Some
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import org.apache.spark.sql.{ SQLContext, SchemaRDD }
import com.intel.event.EventLogging

class SparkFrameStorage(frameFileStorage: FrameFileStorage,
                        maxRows: Int,
                        val metaStore: SlickMetaStoreComponent#SlickMetaStore,
                        sparkAutoPartitioner: SparkAutoPartitioner,
                        getContext: (UserPrincipal) => SparkContext)
    extends FrameStorage with EventLogging with ClassLoaderAware {

  import Rows.Row

  override def expectFrame(frameId: Long): DataFrame = {
    lookup(frameId).getOrElse(throw new NotFoundException("dataframe", frameId.toString))
  }

  override def expectFrame(frameRef: FrameReference): DataFrame = expectFrame(frameRef.id)

  /**
   * Create a FrameRDD or throw an exception if bad frameId is given
   * @param ctx spark context
   * @param frameId primary key of the frame record
   * @return the newly created RDD
   */
  def loadFrameRdd(ctx: SparkContext, frameId: Long): FrameRDD = {
    val frame = lookup(frameId).getOrElse(
      throw new IllegalArgumentException(s"No such data frame: $frameId"))
    loadFrameRdd(ctx, frame)
  }

  /**
   * Create an FrameRDD from a frame data file
   * @param ctx spark context
   * @param frame the model for the frame
   * @return the newly created FrameRDD
   */
  def loadFrameRdd(ctx: SparkContext, frame: DataFrame): FrameRDD = {
    if (frame.revision == 0) {
      // revision zero is special and means nothing has been saved to disk yet
      new FrameRDD(frame.schema, ctx.parallelize[Row](Nil))
    }
    else {
      val absPath = frameFileStorage.currentFrameRevision(frame)
      val f: FrameRDD = if (isParquet(frame)) {
        val sqlContext = new SQLContext(ctx)
        val rows = sqlContext.parquetFile(absPath.toString)
        new FrameRDD(frame.schema, rows)
      }
      else {
        val rows = ctx.objectFile[Row](absPath.toString, sparkAutoPartitioner.partitionsForFile(absPath.toString))
        new FrameRDD(frame.schema, rows)
      }
      f
    }
  }

  /**
   * Determine if a dataFrame is saved as parquet
   * @param frame the data frame to verify
   * @return true if the data frame is saved in the parquet format
   */
  def isParquet(frame: DataFrame): Boolean = {
    frameFileStorage.isParquet(frame)
  }

  /**
   * Save a FrameRDD to HDFS - this is the only save path that should be used
   * @param frameEntity DataFrame representation
   * @param frameRdd the RDD
   * @param rowCount optionally provide the row count if you need to update it
   */
  def saveFrame(frameEntity: DataFrame, frameRdd: FrameRDD, rowCount: Option[Long] = None): DataFrame = {
    val oldRevision = frameEntity.revision
    val nextRevision = frameEntity.revision + 1

    val path = frameFileStorage.createFrameRevision(frameEntity, nextRevision)

    val schemaRDD = frameRdd.toSchemaRDD()
    schemaRDD.saveAsParquetFile(path.toString)

    metaStore.withSession("frame.saveFrame") {
      implicit session =>
        {
          if (frameRdd.schema != null) {
            metaStore.frameRepo.updateSchema(frameEntity, frameRdd.schema.columns)
          }
          if (rowCount.isDefined) {
            metaStore.frameRepo.updateRowCount(frameEntity, rowCount.get)
          }
          metaStore.frameRepo.updateRevision(frameEntity, nextRevision)
        }
    }

    frameFileStorage.deleteFrameRevision(frameEntity, oldRevision)

    expectFrame(frameEntity.id)
  }

  def getPagedRowsRDD(frame: DataFrame, offset: Long, count: Int, ctx: SparkContext)(implicit user: UserPrincipal): RDD[Row] =
    withContext("frame.getPagedRowsRDD") {
      require(frame != null, "frame is required")
      require(offset >= 0, "offset must be zero or greater")
      require(count > 0, "count must be zero or greater")
      withMyClassLoader {
        val rdd: RDD[Row] = loadFrameRdd(ctx, frame.id)
        val rows = MiscFrameFunctions.getPagedRdd[Row](rdd, offset, count, -1)
        rows
      }
    }

  /**
   * Retrieve records from the given dataframe
   * @param frame Frame to retrieve records from
   * @param offset offset in frame before retrieval
   * @param count number of records to retrieve
   * @param user logged in user
   * @return records in the dataframe starting from offset with a length of count
   */
  override def getRows(frame: DataFrame, offset: Long, count: Int)(implicit user: UserPrincipal): Iterable[Row] =
    withContext("frame.getRows") {
      require(frame != null, "frame is required")
      require(offset >= 0, "offset must be zero or greater")
      require(count > 0, "count must be zero or greater")
      withMyClassLoader {
        val absPath = frameFileStorage.currentFrameRevision(frame)
        val reader = new ParquetReader(absPath, frameFileStorage.hdfs)
        val rows = reader.take(count, offset, Some(maxRows))
        rows
      }
    }

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
    frameFileStorage.delete(frame)
    metaStore.withSession("frame.drop") {
      implicit session =>
        {
          metaStore.frameRepo.delete(frame.id)
          Unit

        }
    }
  }

  override def dropColumns(frame: DataFrame, columnIndex: Seq[Int])(implicit user: UserPrincipal): DataFrame =
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
          val check = metaStore.frameRepo.lookupByName(newName)
          if (check.isDefined) {

            //metaStore.frameRepo.scan(0,20).foreach(println)
            throw new RuntimeException("Frame with same name exists. Rename aborted.")
          }
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
    val frameRdd = loadFrameRdd(ctx, frame)
    val errorFrameRdd = loadFrameRdd(ctx, errorFrame)
    new ParseResultRddWrapper(frameRdd, errorFrameRdd)
  }

  override def lookup(id: Long): Option[DataFrame] = {
    metaStore.withSession("frame.lookup") {
      implicit session =>
        {
          metaStore.frameRepo.lookup(id)
        }
    }
  }

  override def getFrames()(implicit user: UserPrincipal): Seq[DataFrame] = {
    metaStore.withSession("frame.getFrames") {
      implicit session =>
        {
          metaStore.frameRepo.scanAll()
        }
    }
  }

  override def create(frameTemplate: DataFrameTemplate)(implicit user: UserPrincipal): DataFrame = {
    metaStore.withSession("frame.createFrame") {
      implicit session =>
        {
          val check = metaStore.frameRepo.lookupByName(frameTemplate.name)
          if (check.isDefined) {
            throw new RuntimeException("Frame with same name exists. Create aborted.")
          }
          val frame = metaStore.frameRepo.insert(frameTemplate).get

          //remove any existing artifacts to prevent collisions when a database is reinitialized.
          frameFileStorage.delete(frame)

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
            val errorTemplate = new DataFrameTemplate(frame.name + "-parse-errors", Some("This frame was automatically created to capture parse errors for " + frame.name))
            val newlyCreateErrorFrame = metaStore.frameRepo.insert(errorTemplate).get
            metaStore.frameRepo.updateErrorFrameId(frame, Some(newlyCreateErrorFrame.id))

            //remove any existing artifacts to prevent collisions when a database is reinitialized.
            frameFileStorage.delete(newlyCreateErrorFrame)

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

  /**
   * Automatically generate a name for a frame.
   *
   * The frame name comprises of the prefix "frame_", a random uuid, and an optional annotation.
   *
   * @param annotation Optional annotation to add to frame name
   * @return Frame name
   */
  def generateFrameName(annotation: Option[String] = None): String = {
    "frame_" + java.util.UUID.randomUUID().toString + annotation.getOrElse("")
  }

}
