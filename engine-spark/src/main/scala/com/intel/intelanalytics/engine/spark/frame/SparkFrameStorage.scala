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

import java.util.UUID

import com.intel.event.EventLogging
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.EntityManager
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.engine.{ FrameStorage, _ }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark._
import com.intel.intelanalytics.engine.spark.frame.parquet.ParquetReader
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.repository.SlickMetaStoreComponent
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.{ DuplicateNameException, NotFoundException }
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.util.control.NonFatal

class SparkFrameStorage(frameFileStorage: FrameFileStorage,
                        maxRows: Int,
                        val metaStore: SlickMetaStoreComponent#SlickMetaStore,
                        sparkAutoPartitioner: SparkAutoPartitioner,
                        getContext: (UserPrincipal) => SparkContext)
    extends FrameStorage with EventLogging with ClassLoaderAware { storage =>

  override type Context = SparkContext
  override type Data = FrameRDD

  object SparkFrameManagement extends EntityManager[FrameEntity.type] {

    override implicit val referenceTag = FrameEntity.referenceTag

    override type Reference = FrameReference

    override type MetaData = FrameMeta

    override type Data = SparkFrameData

    override def getData(reference: Reference)(implicit invocation: Invocation): Data = {
      val meta = getMetaData(reference)
      new SparkFrameData(meta.meta, storage.loadFrameData(sc, meta.meta))
    }

    override def getMetaData(reference: Reference): MetaData = new FrameMeta(expectFrame(reference.id))

    override def create()(implicit invocation: Invocation): Reference = storage.create(DataFrameTemplate(generateFrameName()))

    override def getReference(id: Long): Reference = expectFrame(id)

    implicit def frameToRef(frame: DataFrame): Reference = FrameReference(frame.id, Some(true))

    implicit def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

    implicit def user(implicit invocation: Invocation): UserPrincipal = invocation.user

  }

  EntityRegistry.register(FrameEntity, SparkFrameManagement)

  def exchangeNames(frame1: DataFrame, frame2: DataFrame): Unit = {
    metaStore.withTransaction("SFS.exchangeNames") { implicit txn =>
      val f1Name = frame1.name
      val f2Name = frame2.name
      metaStore.frameRepo.update(frame1.copy(name = UUID.randomUUID().toString))
      metaStore.frameRepo.update(frame2.copy(name = UUID.randomUUID().toString))
      metaStore.frameRepo.update(frame1.copy(name = f2Name))
      metaStore.frameRepo.update(frame2.copy(name = f1Name))
    }
    ()
  }

  import com.intel.intelanalytics.engine.Rows.Row

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
  def loadFrameData(ctx: SparkContext, frameId: Long): FrameRDD = {
    val frame = expectFrame(frameId)
    loadFrameData(ctx, frame)
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
  def loadFrameData(ctx: SparkContext, frame: DataFrame): FrameRDD = {
    val sqlContext = new SQLContext(ctx)
    (frame.storageFormat, frame.storageLocation) match {
      case (_, None) | (None, _) =>
        //  nothing has been saved to disk yet)
        new FrameRDD(frame.schema, ctx.parallelize[Row](Nil))
      case (Some("file/parquet"), Some(absPath)) =>
        val sqlContext = new SQLContext(ctx)
        val rows = sqlContext.parquetFile(absPath.toString)
        new FrameRDD(frame.schema, rows)
      case (Some("file/sequence"), Some(absPath)) =>
        val rows = ctx.objectFile[Row](absPath.toString, sparkAutoPartitioner.partitionsForFile(absPath.toString))
        new FrameRDD(frame.schema, rows)
      case (Some(storage), _) => illegalArg(s"Cannot load frame with storage")
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
  def loadLegacyFrameRdd(ctx: SparkContext, frame: DataFrame): LegacyFrameRDD =
    loadFrameData(ctx, frame).toLegacyFrameRDD

  private def copyParentNameToChildAndRenameParent(frame: DataFrame)(implicit session: metaStore.Session) = {
    require(frame.parent.isDefined, "Cannot copy name from frame parent if no parent provided")

    val parentFrame = expectFrame(frame.parent.get)
    val name = parentFrame.name
    metaStore.frameRepo.update(parentFrame.copy(name = generateFrameName(Option(name))))
    metaStore.frameRepo.update(frame.copy(name = name))

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
    saveFrameData(frameEntity, legacyFrameRdd.toFrameRDD(), rowCount)
  }

  /**
   * Save a FrameRDD to HDFS.
   *
   * This is our preferred path for saving RDDs as data frames.
   *
   * @param frameEntity DataFrame representation
   * @param frameRDD the RDD
   * @param rowCount the number of rows in the RDD
   */
  def saveFrameData(frameEntity: DataFrame, frameRDD: FrameRDD, rowCount: Option[Long] = None): DataFrame =
    withContext("SFS.saveFrame") {

      val path = frameFileStorage.createFrame(frameEntity).toString
      val count = rowCount.getOrElse {
        frameRDD.cache()
        frameRDD.count()
      }
      try {

        val storage = frameEntity.storageFormat.getOrElse("file/parquet")
        storage match {
          case "file/sequence" =>
            val schemaRDD = frameRDD.toSchemaRDD
            schemaRDD.saveAsObjectFile(path)
          case "file/parquet" =>
            val schemaRDD = frameRDD.toSchemaRDD
            schemaRDD.saveAsParquetFile(path.toString)
          case format => illegalArg(s"Unrecognized storage format: $format")
        }

        metaStore.withSession("frame.saveFrame") {
          implicit session =>
            {
              val newFrame = metaStore.frameRepo.update(frameEntity.copy(
                rowCount = Some(count),
                schema = frameRDD.schema,
                storageFormat = Some(storage),
                storageLocation = Some(path)))
              newFrame.get
            }
        }
      }
      catch {
        case NonFatal(e) =>
          error("Error occurred, rolling back creation of file for frame data")
          frameFileStorage.delete(frameEntity)
          throw e
      }
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
        val ctx = getContext(user)
        try {
          val rdd: RDD[Row] = loadLegacyFrameRdd(ctx, frame)
          val rows = MiscFrameFunctions.getRows(rdd, offset, count, maxRows)
          rows
        }
        finally {
          ctx.stop()
        }
        val absPath: Path = new Path(frame.storageLocation.get)
        val reader = new ParquetReader(absPath, frameFileStorage.hdfs)
        val rows = reader.take(count, offset, Some(maxRows))
        rows
      }
    }

  override def drop(frame: DataFrame): Unit = {

    //TODO: validate the args

    //TODO: parse for wild card characters

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
   * Get the pair of LegacyFrameRDD's that were the result of a parse
   * @param ctx spark context
   * @param frame the model of the frame that was the successfully parsed lines
   * @param errorFrame the model for the frame that was the parse errors
   */
  def getParseResult(ctx: SparkContext, frame: DataFrame, errorFrame: DataFrame): ParseResultRddWrapper = {
    val frameRdd = loadLegacyFrameRdd(ctx, frame)
    val errorFrameRdd = loadLegacyFrameRdd(ctx, errorFrame)
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

  override def create(frameTemplate: DataFrameTemplate = DataFrameTemplate(UUID.randomUUID().toString))(implicit user: UserPrincipal): DataFrame = {
    metaStore.withSession("frame.createFrame") {
      implicit session =>
        {

          metaStore.frameRepo.lookupByName(frameTemplate.name).foreach { existingFrame =>
            throw new DuplicateNameException("frame", frameTemplate.name, "Frame with same name exists. Create aborted.")
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
      metaStore.withTransaction("frame.lookupOrCreateErrorFrame") {
        implicit session =>
          {
            val errorTemplate = new DataFrameTemplate(frame.name + "-parse-errors",
              Some("This frame was automatically created to capture parse errors for " + frame.name))
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
