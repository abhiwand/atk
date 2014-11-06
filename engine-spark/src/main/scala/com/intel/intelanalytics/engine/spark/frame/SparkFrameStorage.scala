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
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import DataTypes.DataType
import java.nio.file.Paths
import com.intel.intelanalytics.engine.spark.frame.parquet.ParquetReader
import org.apache.spark.sql.execution.ExistingRdd

import com.intel.event.EventLogging
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.{ HasData, UriReference, EntityManager }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.engine.{ FrameStorage, _ }
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
import org.apache.spark.sql.{ SQLContext, SchemaRDD }
import com.intel.event.EventLogging
import scala.util.parsing.combinator.RegexParsers
import scala.util.{ Failure, Success, Try }

import scala.util.control.NonFatal

class SparkFrameStorage(frameFileStorage: FrameFileStorage,
                        maxRows: Int,
                        val metaStore: SlickMetaStoreComponent#SlickMetaStore,
                        sparkAutoPartitioner: SparkAutoPartitioner,
                        getContext: (UserPrincipal) => SparkContext)
    extends FrameStorage with EventLogging with ClassLoaderAware {
  storage =>

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

    /**
     * Save data of the given type, possibly creating a new object.
     */
    override def saveData(data: Data)(implicit invocation: Invocation): Data = {
      val meta = saveFrameData(data.meta, data.data)
      new SparkFrameData(meta, data.data)
    }
  }

  EntityRegistry.register(FrameEntity, SparkFrameManagement)

  def exchangeNames(frame1: DataFrame, frame2: DataFrame): (DataFrame, DataFrame) = {
    metaStore.withTransaction("SFS.exchangeNames") { implicit txn =>
      val f1Name = frame1.name
      val f2Name = frame2.name
      metaStore.frameRepo.update(frame1.copy(name = UUID.randomUUID().toString))
      metaStore.frameRepo.update(frame2.copy(name = UUID.randomUUID().toString))
      val newF1 = metaStore.frameRepo.update(frame1.copy(name = f2Name))
      val newF2 = metaStore.frameRepo.update(frame2.copy(name = f1Name))
      (newF1.get, newF2.get)
    }
  }

  def exchangeGraphs(frame1: DataFrame, frame2: DataFrame): (DataFrame, DataFrame) = {
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
  def loadFrameData(ctx: SparkContext, frameId: Long)(implicit user: UserPrincipal): FrameRDD = {
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
  def loadFrameData(ctx: SparkContext, frame: DataFrame)(implicit user: UserPrincipal): FrameRDD = {
    val sqlContext = new SQLContext(ctx)
    (frame.storageFormat, frame.storageLocation) match {
      case (_, None) | (None, _) =>
        //  nothing has been saved to disk yet)
        new FrameRDD(frame.schema, ctx.parallelize[sql.Row](Nil))
      case (Some("file/parquet"), Some(absPath)) =>
        val sqlContext = new SQLContext(ctx)
        val rows = sqlContext.parquetFile(absPath.toString)
        new FrameRDD(frame.schema, rows)
      case (Some("file/sequence"), Some(absPath)) =>
        val rows = ctx.objectFile[Row](absPath.toString, sparkAutoPartitioner.partitionsForFile(absPath.toString))
        new LegacyFrameRDD(frame.schema, rows).toFrameRDD()
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
  def loadLegacyFrameRdd(ctx: SparkContext, frameId: Long)(implicit user: UserPrincipal): LegacyFrameRDD = {
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
  def loadLegacyFrameRdd(ctx: SparkContext, frame: DataFrame)(implicit user: UserPrincipal): LegacyFrameRDD =
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
  def saveLegacyFrame(frameEntity: DataFrame, legacyFrameRdd: LegacyFrameRDD, rowCount: Option[Long] = None)(implicit user: UserPrincipal): DataFrame = {
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
  def saveFrameData(frameEntity: DataFrame, frameRDD: FrameRDD, rowCount: Option[Long] = None, parent: Option[DataFrame] = None)(implicit user: UserPrincipal): DataFrame =
    withContext("SFS.saveFrame") {

      val entity = expectFrame(frameEntity.id)

      if (entity.storageLocation.isDefined || frameFileStorage.frameBaseDirectoryExists(entity)) {
        info(s"Path for frame ${entity.id} / ${entity.name} already exists, creating new frame instead")
        //We're saving over something that already exists - which we must not do.
        //So instead we create a new frame.
        val newFrame = create()
        return saveFrameData(newFrame, frameRDD, rowCount, Some(entity))
      }
      info(s"Path for frame ${entity.id} / ${entity.name} does not exist, will save there")

      parent.foreach { p =>
        //We copy the name from the old frame, since this was intended to replace it.
        metaStore.withTransaction("sfs.switch-names-and-graphs") { implicit txn =>
          val (f1, f2) = exchangeNames(entity, p)
          val (f1G, _) = exchangeGraphs(f1, f2)
          f1G
        }
        //TODO: Name maintenance really ought to be moved to CommandExecutor and made more general
      }
      val path = frameFileStorage.frameBaseDirectory(entity.id).toString
      val count = rowCount.getOrElse {
        frameRDD.cache()
        frameRDD.count()
      }
      try {

        val storage = entity.storageFormat.getOrElse("file/parquet")
        storage match {
          case "file/sequence" =>
            val schemaRDD = frameRDD.toSchemaRDD
            schemaRDD.saveAsObjectFile(path)
          case "file/parquet" =>
            val schemaRDD = frameRDD.toSchemaRDD
            schemaRDD.saveAsParquetFile(path)
          case format => illegalArg(s"Unrecognized storage format: $format")
        }

        metaStore.withSession("frame.saveFrame") {
          implicit session =>
            {
              val existing = metaStore.frameRepo.lookup(entity.id).get
              val newFrame = metaStore.frameRepo.update(existing.copy(
                rowCount = Some(count),
                schema = frameRDD.schema,
                storageFormat = Some(storage),
                storageLocation = Some(path),
                parent = parent.map(p => p.id)))
              newFrame.get
            }
        }
      }
      catch {
        case NonFatal(e) =>
          error("Error occurred, rolling back creation of file for frame data")
          frameFileStorage.delete(entity)
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
  def getParseResult(ctx: SparkContext, frame: DataFrame, errorFrame: DataFrame)(implicit user: UserPrincipal): ParseResultRddWrapper = {
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
   * @return the updated frame, plus another frame with parse errors for the 'good' frame
   */
  override def lookupOrCreateErrorFrame(frame: DataFrame): (DataFrame, DataFrame) = {
    val errorFrame = lookupErrorFrame(frame)
    if (!errorFrame.isDefined) {
      val (f, err) = metaStore.withTransaction("frame.lookupOrCreateErrorFrame") { implicit session =>
        val errorTemplate = new DataFrameTemplate(frame.name + "-parse-errors",
          Some("This frame was automatically created to capture parse errors for " + frame.name))
        val newlyCreatedErrorFrame = metaStore.frameRepo.insert(errorTemplate).get
        debug(s"Created error frame ${newlyCreatedErrorFrame.id} for frame ${frame.id}")
        val updated = metaStore.frameRepo.updateErrorFrameId(frame, Some(newlyCreatedErrorFrame.id))
        debug(s"Updated frame ${frame.id} to have error frame ${newlyCreatedErrorFrame.id}")

        //remove any existing artifacts to prevent collisions when a database is reinitialized.
        frameFileStorage.delete(newlyCreatedErrorFrame)

        (updated, newlyCreatedErrorFrame)
      }
      debug("Error frame transaction complete")
      (f, err)
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
  def generateFrameName(annotation: Option[String] = None, prefix: String = "frame_"): String = {
    prefix + java.util.UUID.randomUUID().toString.filterNot(c => c == '-') + annotation.getOrElse("")
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
    Try {
      work(frame)
    } match {
      case Success(f) => f
      case Failure(e) =>
        drop(frame)
        throw e
    }
  }
}
