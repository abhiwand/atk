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
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Status, Naming, EntityManager }
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.frame.{ FrameReference, DataFrameTemplate, FrameEntity }
import com.intel.intelanalytics.engine.FrameStorage
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark._
import com.intel.intelanalytics.engine.spark.frame.parquet.ParquetReader
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.repository.SlickMetaStoreComponent
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.{ EventLoggingImplicits, DuplicateNameException, NotFoundException }
import org.apache.hadoop.fs.Path
import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.intel.event.{ EventContext, EventLogging }
import org.joda.time.DateTime
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import scala.language.implicitConversions

class SparkFrameStorage(frameFileStorage: FrameFileStorage,
                        maxRows: Int,
                        val metaStore: SlickMetaStoreComponent#SlickMetaStore,
                        sparkAutoPartitioner: SparkAutoPartitioner)
    extends FrameStorage with EventLogging with EventLoggingImplicits with ClassLoaderAware {
  storage =>

  override type Context = SparkContext
  override type Data = FrameRDD

  object SparkFrameManagement extends EntityManager[FrameEntityType.type] {

    override implicit val referenceTag = FrameEntityType.referenceTag

    override type Reference = FrameReference

    override type MetaData = FrameMeta

    override type Data = SparkFrameData

    override def getData(reference: Reference)(implicit invocation: Invocation): Data = {
      val meta = getMetaData(reference)
      new SparkFrameData(meta.meta, storage.loadFrameData(sc, meta.meta))
    }

    override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new FrameMeta(expectFrame(reference.id))

    override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference =
      storage.create(args)

    override def getReference(id: Long)(implicit invocation: Invocation): Reference = expectFrame(id)

    implicit def frameToRef(frame: FrameEntity)(implicit invocation: Invocation): Reference = FrameReference(frame.id)

    implicit def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

    /**
     * Save data of the given type, possibly creating a new object.
     */
    override def saveData(data: Data)(implicit invocation: Invocation): Data = {
      val meta = saveFrameData(data.meta, data.data)
      new SparkFrameData(meta, data.data)
    }

    /**
     * Creates an (empty) instance of the given type, reserving a URI
     */
    override def delete(reference: SparkFrameStorage.this.SparkFrameManagement.Reference)(implicit invocation: Invocation): Unit = {
      val meta = getMetaData(reference)
      drop(meta.meta)
    }
  }

  EntityTypeRegistry.register(FrameEntityType, SparkFrameManagement)

  def exchangeNames(frame1: FrameEntity, frame2: FrameEntity): (FrameEntity, FrameEntity) = {
    metaStore.withTransaction("SFS.exchangeNames") { implicit txn =>
      val f1Name = frame1.name
      val f2Name = frame2.name
      metaStore.frameRepo.update(frame1.copy(name = None))
      metaStore.frameRepo.update(frame2.copy(name = None))
      val newF1 = metaStore.frameRepo.update(frame1.copy(name = f2Name))
      val newF2 = metaStore.frameRepo.update(frame2.copy(name = f1Name))
      (newF1.get, newF2.get)
    }
  }

  def exchangeGraphs(frame1: FrameEntity, frame2: FrameEntity): (FrameEntity, FrameEntity) = {
    metaStore.withTransaction("SFS.exchangeGraphs") { implicit txn =>
      val f1Graph = frame1.graphId
      val f2Graph = frame2.graphId
      metaStore.frameRepo.update(frame1.copy(graphId = None))
      metaStore.frameRepo.update(frame2.copy(graphId = None))
      val newF1 = metaStore.frameRepo.update(frame1.copy(graphId = f2Graph))
      val newF2 = metaStore.frameRepo.update(frame2.copy(graphId = f1Graph))
      (newF1.get, newF2.get)
    }
  }

  import com.intel.intelanalytics.engine.Rows.Row

  override def expectFrame(frameId: Long)(implicit invocation: Invocation): FrameEntity = {
    lookup(frameId).getOrElse(throw new NotFoundException("frame", frameId.toString))
  }

  override def expectFrame(frameRef: FrameReference)(implicit invocation: Invocation): FrameEntity = expectFrame(frameRef.id)

  /**
   * Create an FrameRDD from a frame data file
   *
   * This is our preferred format for loading frames as RDDs.
   *
   * @param ctx spark context
   * @param frameId the id for the frame
   * @return the newly created FrameRDD
   */
  def loadFrameData(ctx: SparkContext, frameId: Long)(implicit invocation: Invocation): FrameRDD = {
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
  def loadFrameData(ctx: SparkContext, frame: FrameEntity)(implicit invocation: Invocation): FrameRDD = withContext("loadFrameRDD") {
    (frame.storageFormat, frame.storageLocation) match {
      case (_, None) | (None, _) =>
        //  nothing has been saved to disk yet)
        new FrameRDD(frame.schema, ctx.parallelize[sql.Row](Nil, SparkAutoPartitioner.minDefaultPartitions))
      case (Some("file/parquet"), Some(absPath)) =>
        val sqlContext = new SQLContext(ctx)
        val rows = sqlContext.parquetFile(absPath.toString)
        info(frame.toDebugString + ", partitions:" + rows.partitions.length)
        new FrameRDD(frame.schema, rows)
      case (Some("file/sequence"), Some(absPath)) =>
        val rows = ctx.objectFile[Row](absPath.toString, sparkAutoPartitioner.partitionsForFile(absPath.toString))
        new LegacyFrameRDD(frame.schema, rows).toFrameRDD()
      case (Some(s), _) => illegalArg(s"Cannot load frame with storage '$s'")
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
  @deprecated("use FrameRDD and related methods instead")
  def loadLegacyFrameRdd(ctx: SparkContext, frameId: Long)(implicit invocation: Invocation): LegacyFrameRDD = {
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
  @deprecated("use FrameRDD and related methods instead")
  def loadLegacyFrameRdd(ctx: SparkContext, frame: FrameEntity)(implicit invocation: Invocation): LegacyFrameRDD =
    loadFrameData(ctx, frame).toLegacyFrameRDD

  /**
   * Determine if a dataFrame is saved as parquet
   * @param frame the data frame to verify
   * @return true if the data frame is saved in the parquet format
   */
  def isParquet(frame: FrameEntity): Boolean = {
    frameFileStorage.isParquet(frame)
  }

  /**
   * Save a LegacyFrameRDD to HDFS - this is the only save path that should be used for legacy Frames.
   *
   * Please don't write new code against this legacy format:
   * - This format requires extra maps to read/write Parquet files.
   * - We'd rather use FrameRDD which extends SchemaRDD and can go direct to/from Parquet.
   *
   * @param frame reference to a frame
   * @param legacyFrameRdd the RDD
   */
  @deprecated("use FrameRDD and related methods instead")
  def saveLegacyFrame(frame: FrameReference, legacyFrameRdd: LegacyFrameRDD)(implicit invocation: Invocation): FrameEntity = {
    saveFrameData(frame, legacyFrameRdd.toFrameRDD())
  }

  /**
   * Save a FrameRDD to HDFS.
   *
   * This is our preferred path for saving RDDs as data frames.
   *
   * If the current frame is already materialized, a new entry will be created in the meta data repository, otherwise the existing entry will be updated.
   *
   * @param frame reference to a data frame
   * @param frameRDD the RDD containing the actual data
   */
  override def saveFrameData(frame: FrameReference, frameRDD: FrameRDD)(implicit invocation: Invocation): FrameEntity =
    withContext("SFS.saveFrame") {

      val frameEntity = expectFrame(frame.id)

      // determine if this one has been materialized to disk or not
      val targetEntity = if (frameEntity.storageLocation.isDefined) {
        metaStore.withTransaction("sfs.switch-names-and-graphs") { implicit txn =>
          //We're saving over something that already exists - which we must not do.
          //So instead we create a new frame.
          info(s"Path for frame ${frameEntity.id} / ${frameEntity.name} already exists, creating new frame instead")
          // TODO: initialize command id in frame
          val child = frameEntity.createChild(Some(invocation.user.user.id), command = None, frameRDD.frameSchema)
          metaStore.frameRepo.insert(child)
        }
      }
      else {
        info(s"Path for frame ${frameEntity.id} / ${frameEntity.name} does not exist, will save there")
        frameEntity
      }
      try {

        metaStore.withSession("frame.saveFrame") {
          implicit session =>
            {
              // set the timestamp for when we're starting materialization
              metaStore.frameRepo.update(targetEntity.copy(materializedOn = Some(new DateTime)))
            }
        }

        // delete incomplete data on disk if it exists
        frameFileStorage.delete(targetEntity)

        // save the actual data
        val storageFormat = targetEntity.storageFormat.getOrElse("file/parquet")
        val path = frameFileStorage.frameBaseDirectory(targetEntity.id).toString
        frameRDD.save(path, storageFormat)

        // update the metastore
        metaStore.withSession("frame.saveFrame") {
          implicit session =>
            {
              val frame = expectFrame(targetEntity.id)

              require(frameRDD.frameSchema != null, "frame schema was null, we need developer to add logic to handle this - we used to have this, not sure if we still do --Todd 12/16/2014")

              val updatedFrame = frame.copy(status = Status.Active,
                storageFormat = Some(storageFormat),
                storageLocation = Some(path),
                schema = frameRDD.frameSchema,
                materializationComplete = Some(new DateTime))

              val withCount = updatedFrame.copy(rowCount = Some(getRowCount(updatedFrame)))

              metaStore.frameRepo.update(withCount)
            }
        }

        // if a child was created, it will need to take the name and graph from the parent
        if (frameEntity.id != targetEntity.id) {
          metaStore.withTransaction("sfs.switch-names-and-graphs") { implicit txn =>
            {
              // remove name from existing frame since it is on the child
              val (f1, f2) = exchangeNames(expectFrame(frameEntity.id), expectFrame(targetEntity.id))
              // TODO: shouldn't exchange graphs, should insert a new revision of a graph but this is complicated because there might be multiple frame modifications in one step for graphs
              exchangeGraphs(f1, f2)
            }
          }
        }

        // look up the latest version from the DB
        expectFrame(targetEntity.id)
      }
      catch {
        case NonFatal(e) =>
          error("Error occurred, rolling back saving of frame data", exception = e)
          drop(targetEntity)
          // TODO: later, when we're really lazy, we'll use incomplete status
          //error("Error occurred, rolling back creation of file for frame data, marking frame as Incomplete", exception = e)
          //updateFrameStatus(targetEntity.toReference, Status.Incomplete)
          throw e
      }
    }

  def updateFrameStatus(frame: FrameReference, statusId: Long)(implicit invocation: Invocation): Unit = {
    try {
      metaStore.withSession("frame.updateFrameStatus") {
        implicit session =>
          {
            val entity = expectFrame(frame.id)
            metaStore.frameRepo.update(entity.copy(status = statusId))
          }
      }
    }
    catch {
      case e: Exception => error("Error rolling back frame, exception while trying to mark frame as Incomplete", exception = e)
    }
  }

  def getPagedRowsRDD(frame: FrameEntity, offset: Long, count: Int, ctx: SparkContext)(implicit invocation: Invocation): RDD[Row] =
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
   * @return records in the dataframe starting from offset with a length of count
   */
  override def getRows(frame: FrameEntity, offset: Long, count: Int)(implicit invocation: Invocation): Iterable[Row] =
    withContext("frame.getRows") {
      require(frame != null, "frame is required")
      require(offset >= 0, "offset must be zero or greater")
      require(count > 0, "count must be zero or greater")
      withMyClassLoader {
        val reader = getReader(frame)
        val rows = reader.take(count, offset, Some(maxRows))
        rows
      }
    }

  /**
   * Row count for the supplied frame (assumes Parquet storage)
   * @return row count
   */
  def getRowCount(frame: FrameEntity)(implicit invocation: Invocation): Long = {
    if (frame.storageLocation.isDefined) {
      val reader = getReader(frame)
      reader.rowCount()
    }
    else {
      // TODO: not sure what to do if nothing is persisted?
      throw new NotImplementedError("trying to get a row count on a frame that hasn't been persisted, not sure what to do")
    }
  }

  def getReader(frame: FrameEntity)(implicit invocation: Invocation): ParquetReader = {
    withContext("frame.getReader") {
      require(frame != null, "frame is required")
      withMyClassLoader {
        val absPath: Path = new Path(frame.storageLocation.get)
        new ParquetReader(absPath, frameFileStorage.hdfs)
      }
    }
  }

  override def drop(frame: FrameEntity)(implicit invocation: Invocation): Unit = {
    frameFileStorage.delete(frame)
    metaStore.withSession("frame.drop") {
      implicit session =>
        {
          metaStore.frameRepo.delete(frame.id).get
        }
    }
  }

  override def renameFrame(frame: FrameEntity, newName: String)(implicit invocation: Invocation): FrameEntity = {
    metaStore.withSession("frame.rename") {
      implicit session =>
        {
          val check = metaStore.frameRepo.lookupByName(Some(newName))
          if (check.isDefined) {

            //metaStore.frameRepo.scan(0,20).foreach(println)
            throw new RuntimeException("Frame with same name exists. Rename aborted.")
          }
          val newFrame = frame.copy(name = Some(newName))
          metaStore.frameRepo.update(newFrame).get
        }
    }
  }

  override def renameColumns(frame: FrameEntity, name_pairs: Seq[(String, String)])(implicit invocation: Invocation): FrameEntity =
    metaStore.withSession("frame.renameColumns") {
      implicit session =>
        {
          metaStore.frameRepo.updateSchema(frame, frame.schema.renameColumns(name_pairs.toMap))
        }
    }

  override def lookupByName(name: Option[String])(implicit invocation: Invocation): Option[FrameEntity] = {
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
  def getParseResult(ctx: SparkContext, frame: FrameEntity, errorFrame: FrameEntity)(implicit invocation: Invocation): ParseResultRddWrapper = {
    val frameRdd = loadFrameData(ctx, frame)
    val errorFrameRdd = loadFrameData(ctx, errorFrame)
    new ParseResultRddWrapper(frameRdd, errorFrameRdd)
  }

  override def lookup(id: Long)(implicit invocation: Invocation): Option[FrameEntity] = {
    metaStore.withSession("frame.lookup") {
      implicit session =>
        {
          metaStore.frameRepo.lookup(id)
        }
    }
  }

  override def getFrames()(implicit invocation: Invocation): Seq[FrameEntity] = {
    metaStore.withSession("frame.getFrames") {
      implicit session =>
        {
          metaStore.frameRepo.scanAll().filter(f => f.status != Status.Deleted && f.status != Status.Dead && f.name.isDefined)
        }
    }
  }

  override def create(arguments: CreateEntityArgs = CreateEntityArgs())(implicit invocation: Invocation): FrameEntity = {
    metaStore.withSession("frame.createFrame") {
      implicit session =>
        {
          if (arguments.name != None) {
            metaStore.frameRepo.lookupByName(arguments.name).foreach {
              existingFrame =>
                throw new DuplicateNameException("frame", arguments.name.get, "Frame with same name exists. Create aborted.")
            }
          }
          val frameTemplate = DataFrameTemplate(arguments.name)
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
   * @return the updated frame, plus another frame with parse errors for the 'good' frame
   */
  override def lookupOrCreateErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): (FrameEntity, FrameEntity) = {
    val errorFrame = lookupErrorFrame(frame)
    if (!errorFrame.isDefined) {
      metaStore.withSession("frame.lookupOrCreateErrorFrame") {
        implicit session =>
          {
            val errorTemplate = new DataFrameTemplate(None, Some(s"This frame was automatically created to capture parse errors for ${frame.name} ID: ${frame.id}"))
            val newlyCreatedErrorFrame = metaStore.frameRepo.insert(errorTemplate).get
            val updated = metaStore.frameRepo.updateErrorFrameId(frame, Some(newlyCreatedErrorFrame.id))

            //remove any existing artifacts to prevent collisions when a database is reinitialized.
            frameFileStorage.delete(newlyCreatedErrorFrame)

            (updated, newlyCreatedErrorFrame)
          }
      }
    }
    else {
      (frame, errorFrame.get)
    }
  }

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  override def lookupErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): Option[FrameEntity] = {
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
   * @param args - create entity arguments
   * @param work - Frame to Frame function.  This function typically loads RDDs, does work, and saves RDDS
   * @return - the frame result of work if successful, otherwise an exception is raised
   */
  // TODO: change to return a Try[DataFrame] instead of raising exception?
  def tryNewFrame(args: CreateEntityArgs = CreateEntityArgs())(work: FrameEntity => FrameEntity)(implicit invocation: Invocation): FrameEntity = {
    val frame = create(args)
    Try {
      work(frame)
    } match {
      case Success(f) => f
      case Failure(e) =>
        drop(frame)
        throw e
    }
  }

  /**
   * Set the last read date for a frame to the current time
   * @param frame the frame to update
   */
  def updateLastReadDate(frame: FrameEntity): Option[FrameEntity] = {
    metaStore.withSession("frame.updateLastReadDate") {
      implicit session =>
        {
          if (frame.graphId != None) {
            val graph = metaStore.graphRepo.lookup(frame.graphId.get).get
            metaStore.graphRepo.updateLastReadDate(graph)
          }
          metaStore.frameRepo.updateLastReadDate(frame).toOption
        }
    }
  }
}
