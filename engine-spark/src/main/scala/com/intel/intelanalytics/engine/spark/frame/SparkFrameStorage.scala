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
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import DataTypes.DataType
import java.nio.file.Paths
import com.intel.intelanalytics.engine.spark.frame.parquet.ParquetReader
import org.apache.spark.sql.execution.ExistingRdd

import scala.io.Codec
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark._
import org.apache.spark.{ sql, SparkContext }
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
import scala.util.parsing.combinator.RegexParsers
import scala.util.{ Failure, Success, Try }

class SparkFrameStorage(frameFileStorage: FrameFileStorage,
                        maxRows: Int,
                        val metaStore: SlickMetaStoreComponent#SlickMetaStore,
                        sparkAutoPartitioner: SparkAutoPartitioner,
                        getContext: (UserPrincipal) => SparkContext)
    extends FrameStorage with EventLogging with ClassLoaderAware {

  import Rows.Row

  override def expectFrame(frameId: Long): DataFrame = {
    lookup(frameId).getOrElse(throw new NotFoundException("frame", frameId.toString))
  }

  override def expectFrame(frameRef: FrameReference): DataFrame = expectFrame(frameRef.id)

  /**
   * Create an FrameRDD from a frame data file
   *
   * This is our preferred format for loading frames as RDDs.
   *
   * @param ctx spark context
   * @param frameId the id for the frame
   * @return the newly created FrameRDD
   */
  def loadFrameRDD(ctx: SparkContext, frameId: Long): FrameRDD = {
    val frame = lookup(frameId).getOrElse(
      throw new IllegalArgumentException(s"No such data frame: $frameId"))
    loadFrameRDD(ctx, frame)
  }

  /**
   * Create an FrameRDD from a frame data file.
   *
   * This is our preferred format for loading frames as RDDs.
   *
   * @param ctx spark context
   * @param frame the model for the frame
   * @return the newly created FrameRDD
   */
  def loadFrameRDD(ctx: SparkContext, frame: DataFrame): FrameRDD = {
    val sqlContext = new SQLContext(ctx)
    if (frame.revision == 0) {
      // revision zero is special and means nothing has been saved to disk yet)
      new FrameRDD(frame.schema, ctx.parallelize[sql.Row](Nil))
    }
    else {
      val absPath = frameFileStorage.currentFrameRevision(frame)
      if (!isParquet(frame))
        throw new IllegalStateException(s"Frame: ${frame.id} is not stored in the parquet format")
      val sqlContext = new SQLContext(ctx)
      val rows = sqlContext.parquetFile(absPath.toString)
      new FrameRDD(frame.schema, rows)
    }
  }

  /**
   * Create a LegacyFrameRDD or throw an exception if bad frameId is given.
   *
   * Please don't write new code against this legacy format:
   * - This format requires extra maps to read/write Parquet files.
   * - We'd rather use FrameRDD which extends SchemaRDD and can go direct to/from Parquet.
   *
   * @param ctx spark context
   * @param frameId primary key of the frame record
   * @return the newly created RDD
   */
  def loadLegacyFrameRdd(ctx: SparkContext, frameId: Long): LegacyFrameRDD = {
    val frame = lookup(frameId).getOrElse(
      throw new IllegalArgumentException(s"No such data frame: $frameId"))
    loadLegacyFrameRdd(ctx, frame)
  }

  /**
   * Create an LegacyFrameRDD from a frame data file
   *
   * Please don't write new code against this legacy format:
   * - This format requires extra maps to read/write Parquet files.
   * - We'd rather use FrameRDD which extends SchemaRDD and can go direct to/from Parquet.
   *
   * @param ctx spark context
   * @param frame the model for the frame
   * @return the newly created FrameRDD
   */
  def loadLegacyFrameRdd(ctx: SparkContext, frame: DataFrame): LegacyFrameRDD = {
    if (frame.revision == 0) {
      // revision zero is special and means nothing has been saved to disk yet
      new LegacyFrameRDD(frame.schema, ctx.parallelize[Row](Nil))
    }
    else {
      val absPath = frameFileStorage.currentFrameRevision(frame)
      val f: LegacyFrameRDD =
        if (isParquet(frame)) {
          loadFrameRDD(ctx, frame).toLegacyFrameRDD
        }
        else {
          val rows = ctx.objectFile[Row](absPath.toString, sparkAutoPartitioner.partitionsForFile(absPath.toString))
          new LegacyFrameRDD(frame.schema, rows)
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
   * Save a LegacyFrameRDD to HDFS - this is the only save path that should be used for legacy Frames.
   *
   * Please don't write new code against this legacy format:
   * - This format requires extra maps to read/write Parquet files.
   * - We'd rather use FrameRDD which extends SchemaRDD and can go direct to/from Parquet.
   *
   * @param frameEntity DataFrame representation
   * @param legacyFrameRdd the RDD
   * @param rowCount optionally provide the row count if you need to update it
   */
  def saveLegacyFrame(frameEntity: DataFrame, legacyFrameRdd: LegacyFrameRDD, rowCount: Option[Long] = None): DataFrame = {
    saveFrame(frameEntity, legacyFrameRdd.toFrameRDD(), rowCount)
  }

  /**
   * Save a FrameRDD to HDFS.
   *
   * This is our preferred path for saving RDDs as data frames.
   *
   * @param frameEntity DataFrame representation
   * @param frameRDD the RDD
   * @param rowCount optionally provide the row count if you need to update it
   */
  def saveFrame(frameEntity: DataFrame, frameRDD: FrameRDD, rowCount: Option[Long] = None): DataFrame = {
    val oldRevision = frameEntity.revision
    val nextRevision = frameEntity.revision + 1

    val path = frameFileStorage.createFrameRevision(frameEntity, nextRevision)

    frameRDD.saveAsParquetFile(path.toString)

    metaStore.withSession("frame.saveFrame") {
      implicit session =>
        {
          if (frameRDD.frameSchema != null) {
            metaStore.frameRepo.updateSchema(frameEntity, frameRDD.frameSchema)
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
        val rdd: RDD[Row] = loadLegacyFrameRdd(ctx, frame.id)
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

  /**
   * @deprecated schema should be updated when you save a FrameRDD, this method shouldn't be needed
   */
  def updateSchema(frame: DataFrame, schema: Schema): DataFrame = {
    metaStore.withSession("frame.updateSchema") {
      implicit session =>
        {
          metaStore.frameRepo.updateSchema(frame, schema)
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

    //validate the args

    //parse for wild card characters

    frameFileStorage.delete(frame)
    metaStore.withSession("frame.drop") {
      implicit session =>
        {
          metaStore.frameRepo.delete(frame.id)
          Unit

        }
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
    metaStore.withSession("frame.renameColumns") {
      implicit session =>
        {
          metaStore.frameRepo.updateSchema(frame, frame.schema.renameColumns(name_pairs.toMap))
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
   * Get the pair of LegacyFrameRDD's that were the result of a parse
   * @param ctx spark context
   * @param frame the model of the frame that was the successfully parsed lines
   * @param errorFrame the model for the frame that was the parse errors
   */
  def getParseResult(ctx: SparkContext, frame: DataFrame, errorFrame: DataFrame): ParseResultRddWrapper = {
    val frameRdd = loadFrameRDD(ctx, frame)
    val errorFrameRdd = loadFrameRDD(ctx, errorFrame)
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
            val errorTemplate = new DataFrameTemplate(frame.name + "_parse_errors", Some("This frame was automatically created to capture parse errors for " + frame.name))
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
   * Provides a clean-up context to create and work on a new frame
   *
   * Takes the template, creates a frame and then hands it to a work function.  If any error occurs during the work
   * the frame is deleted from the metastore.  Typical usage would be during the creation of a brand new frame,
   * where data processing needs to occur, any error means the frame should not continue to exist in the metastore.
   *
   * @param template - template describing a new frame
   * @param work - Frame to Frame function.  This function typically loads RDDs, does work, and saves RDDS
   * @param user user
   * @return - the frame result of work if successful, otherwise an exception is raised
   */
  // TODO: change to return a Try[DataFrame] instead of raising exception?
  def tryNewFrame(template: DataFrameTemplate)(work: DataFrame => DataFrame)(implicit user: UserPrincipal): DataFrame = {
    val frame = create(template)
    Try { work(frame) } match {
      case Success(f) => f
      case Failure(e) =>
        drop(frame)
        throw e
    }
  }
}
