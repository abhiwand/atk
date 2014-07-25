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

package com.intel.intelanalytics.engine.spark

import java.util.{ ArrayList => JArrayList, List => JList }

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.query.{ Execution => QueryExecution, RowQuery, Query, QueryTemplate }
import com.intel.intelanalytics.domain.command.{ Command, CommandDefinition, CommandTemplate, Execution }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.frame.load.{ LineParserArguments, LineParser, LoadSource, Load }

import com.intel.intelanalytics.domain.graph.{ Graph, GraphLoad, GraphTemplate }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema, SchemaUtil }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import com.intel.intelanalytics.engine.spark.command.CommandExecutor
import com.intel.intelanalytics.engine.spark.queries.{ SparkQueryStorage, QueryExecutor }
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.frame.{ RDDJoinParam, RowParser, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.NotFoundException
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ EnginePythonAccumulatorParam, EnginePythonRDD }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.engine.SparkProgressListener
import org.apache.spark.rdd.RDD
import spray.json._

import com.intel.intelanalytics.domain.frame.LoadLines

import DomainJsonProtocol._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import scala.util.Try
import org.apache.spark.engine.SparkProgressListener
import com.intel.spark.mllib.util.{ LabeledLine, MLDataSplitter }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.Try
import org.apache.spark.engine.SparkProgressListener
import com.intel.intelanalytics.domain.frame.FrameAddColumns
import com.intel.intelanalytics.domain.frame.FrameRenameFrame
import com.intel.intelanalytics.domain.frame.load.LineParserArguments
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.domain.frame.DropDuplicates
import com.intel.intelanalytics.engine.spark.frame.RowParseResult
import com.intel.intelanalytics.domain.frame.FrameProject
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.frame.load.Load
import com.intel.intelanalytics.domain.frame.load.LineParser
import com.intel.intelanalytics.domain.frame.BigColumn
import com.intel.intelanalytics.domain.frame.FrameGroupByColumn
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.FrameRemoveColumn
import com.intel.intelanalytics.engine.spark.frame.RDDJoinParam
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.frame.load.LoadSource
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.engine.ProgressInfo
import com.intel.intelanalytics.domain.frame.FrameRenameColumns
import com.intel.intelanalytics.domain.frame.BinColumn
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.command.Execution
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.domain.frame.FlattenColumn
import com.intel.intelanalytics.domain.frame.FrameJoin

object SparkEngine {
  private val pythonRddDelimiter = "\0"
}

class SparkEngine(sparkContextManager: SparkContextManager,
                  commands: CommandExecutor,
                  commandStorage: CommandStorage,
                  frames: SparkFrameStorage,
                  graphs: GraphStorage,
                  queryStorage: SparkQueryStorage,
                  queries: QueryExecutor) extends Engine
    with EventLogging
    with ClassLoaderAware {

  private val fsRoot = SparkEngineConfig.fsRoot

  /* This progress listener saves progress update to command table */
  SparkProgressListener.progressUpdater = new CommandProgressUpdater {
    /**
     * save the progress update
     * @param commandId id of the command
     * @param progressInfo list of progress for jobs initiated by the command
     */
    override def updateProgress(commandId: Long, progressInfo: List[ProgressInfo]): Unit = commandStorage.updateProgress(commandId, progressInfo)
  }

  def shutdown: Unit = {
    sparkContextManager.cleanup()
  }

  override def getCommands(offset: Int, count: Int): Future[Seq[Command]] = withContext("se.getCommands") {
    future {
      commandStorage.scan(offset, count)
    }
  }

  override def getCommand(id: Long): Future[Option[Command]] = withContext("se.getCommand") {
    future {
      commandStorage.lookup(id)
    }
  }

  /**
   * return a list of the existing queries
   * @param offset First query to obtain.
   * @param count Number of queries to obtain.
   * @return sequence of queries
   */
  override def getQueries(offset: Int, count: Int): Future[Seq[Query]] = withContext("se.getQueries") {
    future {
      queryStorage.scan(offset, count)
    }
  }

  /**
   *  return a query object
   * @param id query id
   * @return Query
   */
  override def getQuery(id: Long): Future[Option[Query]] = withContext("se.getQuery") {
    future {
      queryStorage.lookup(id)
    }
  }

  /**
   * returns the data found in a specific query result page
   *
   * @param id query id
   * @param pageId page id
   * @param user current user
   * @return data of specific page
   */
  override def getQueryPage(id: Long, pageId: Long)(implicit user: UserPrincipal) = withContext("se.getQueryPage") {
    val ctx = sparkContextManager.context(user)
    val data = queryStorage.getQueryPage(ctx.sparkContext, id, pageId)
    data
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @param user the user running the command
   * @return an Execution that can be used to track the completion of the command
   */
  def execute(command: CommandTemplate)(implicit user: UserPrincipal): Execution =
    commands.execute(command, user, implicitly[ExecutionContext])

  /**
   * All the command definitions available
   */
  override def getCommandDefinitions()(implicit user: UserPrincipal): Iterable[CommandDefinition] = {
    commands.getCommandDefinitions()
  }

  def load(arguments: Load)(implicit user: UserPrincipal): Execution =
    commands.execute(loadCommand, arguments, user, implicitly[ExecutionContext])

  val loadCommand = commands.registerCommand(name = "dataframe/load", loadSimple)

  /**
   * Load data from a LoadSource object to an existing destination described in the Load object
   * @param load Load command object
   * @param user current user
   */
  def loadSimple(load: Load, user: UserPrincipal): DataFrame = {
    val frameId = load.destination.id
    val destinationFrame = expectFrame(frameId)
    val ctx = sparkContextManager.context(user)

    if (load.source.isFrame) {
      // load data from an existing frame and add its data onto the target frame
      val additionalData = frames.getFrameRdd(ctx.sparkContext, expectFrame(load.source.uri.toInt))
      unionAndSave(ctx.sparkContext, destinationFrame, additionalData)
    }
    else if (load.source.isFile) {
      val parser = load.source.parser.get
      val parseResult = LoadRDDFunctions.loadAndParseLines(ctx.sparkContext, fsRoot + "/" + load.source.uri, parser)

      // parse failures go to their own data frame
      if (parseResult.errorLines.count() > 0) {
        val errorFrame = frames.lookupOrCreateErrorFrame(destinationFrame)
        unionAndSave(ctx.sparkContext, errorFrame, parseResult.errorLines)
      }

      // successfully parsed lines get added to the destination frame
      unionAndSave(ctx.sparkContext, destinationFrame, parseResult.parsedLines)
    }
    else {
      throw new IllegalArgumentException("Unsupported load source: " + load.source.source_type)
    }

  }

  /**
   * Union the additionalData onto the end of the existingFrame
   * @param sparkContext Spark Context
   * @param existingFrame the target DataFrame that may or may not already have data
   * @param additionalData the data to add to the existingFrame
   * @return the frame with updated schema
   */
  private def unionAndSave(sparkContext: SparkContext, existingFrame: DataFrame, additionalData: FrameRDD): DataFrame = {
    val existingRdd = frames.getFrameRdd(sparkContext, existingFrame)
    val unionedRdd = existingRdd.union(additionalData)
    val location = fsRoot + frames.getFrameDataFile(existingFrame.id)
    val rowCount = unionedRdd.count()
    unionedRdd.rows.saveAsObjectFile(location)
    frames.updateSchema(existingFrame, unionedRdd.schema.columns)
    frames.updateRowCount(existingFrame, rowCount)
  }

  def create(frame: DataFrameTemplate)(implicit user: UserPrincipal): Future[DataFrame] =
    future {
      frames.create(frame)
    }

  def delete(frame: DataFrame): Future[Unit] = withContext("se.delete") {
    future {
      frames.drop(frame)
    }
  }

  def getFrames(offset: Int, count: Int)(implicit p: UserPrincipal): Future[Seq[DataFrame]] = withContext("se.getFrames") {
    future {
      frames.getFrames(offset, count)
    }
  }

  def getFrameByName(name: String)(implicit p: UserPrincipal): Future[Option[DataFrame]] = withContext("se.getFrameByName") {
    future {
      frames.lookupByName(name)
    }
  }

  def expectFrame(frameId: Long): DataFrame = {
    frames.lookup(frameId).getOrElse(throw new NotFoundException("dataframe", frameId.toString))
  }

  def expectFrame(frameRef: FrameReference): DataFrame = expectFrame(frameRef.id)

  def renameFrame(arguments: FrameRenameFrame)(implicit user: UserPrincipal): Execution =
    commands.execute(renameFrameCommand, arguments, user, implicitly[ExecutionContext])

  val renameFrameCommand = commands.registerCommand("dataframe/rename_frame", renameFrameSimple)

  private def renameFrameSimple(arguments: FrameRenameFrame, user: UserPrincipal): DataFrame = {
    val frame = expectFrame(arguments.frame)
    val newName = arguments.new_name
    frames.renameFrame(frame, newName)
  }

  def renameColumns(arguments: FrameRenameColumns[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(renameColumnsCommand, arguments, user, implicitly[ExecutionContext])

  val renameColumnsCommand = commands.registerCommand("dataframe/rename_columns", renameColumnsSimple)
  def renameColumnsSimple(arguments: FrameRenameColumns[JsObject, Long], user: UserPrincipal) = {
    val frameID = arguments.frame
    val frame = expectFrame(frameID)
    frames.renameColumns(frame, arguments.original_names.zip(arguments.new_names))
  }

  def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(projectCommand, arguments, user, implicitly[ExecutionContext])

  val projectCommand = commands.registerCommand("dataframe/project", projectSimple)
  def projectSimple(arguments: FrameProject[JsObject, Long], user: UserPrincipal): DataFrame = {

    implicit val u = user

    val sourceFrameID = arguments.frame
    val sourceFrame = expectFrame(sourceFrameID)
    val projectedFrameID = arguments.projected_frame
    val projectedFrame = expectFrame(projectedFrameID)
    val ctx = sparkContextManager.context(user).sparkContext
    val columns = arguments.columns

    val schema = sourceFrame.schema
    val location = fsRoot + frames.getFrameDataFile(projectedFrameID)

    val columnIndices = for {
      col <- columns
      columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
    } yield columnIndex

    if (columnIndices.contains(-1)) {
      throw new IllegalArgumentException(s"Invalid list of columns: ${arguments.columns.toString()}")
    }

    frames.getFrameRowRdd(ctx, sourceFrameID)
      .map(row => {
        for { i <- columnIndices } yield row(i)
      }.toArray)
      .saveAsObjectFile(location)

    val projectedColumns = arguments.new_column_names match {
      case empty if empty.size == 0 => for { i <- columnIndices } yield schema.columns(i)
      case _ =>
        for { i <- 0 until columnIndices.size }
          yield (arguments.new_column_names(i), schema.columns(columnIndices(i))._2)
    }

    frames.updateSchema(projectedFrame, projectedColumns.toList)
    frames.updateRowCount(projectedFrame, sourceFrame.rowCount)
  }

  /**
   * Randomly assigns sample lables to rows of a table, with probabilities for each label given by an incoming
   * probability distribution. Modifies the current table by adding a  column (called "sample bin" by default) that
   * contains the sample labels.
   *
   * @param arguments AssignSample command payload
   * @param user the current user
   * @return
   */
  def assignSample(arguments: AssignSample)(implicit user: UserPrincipal): Execution =
    commands.execute(assignSampleCommand, arguments, user, implicitly[ExecutionContext])

  val assignSampleCommand = commands.registerCommand("dataframe/assign_sample", assignSampleSimple)

  def assignSampleSimple(arguments: AssignSample, user: UserPrincipal) = {

    val ctx = sparkContextManager.context(user).sparkContext

    val frameID = arguments.frame.id
    val frame = expectFrame(frameID)

    val splitPercentages = arguments.sample_percentages.toArray

    val outputColumn = arguments.output_column.getOrElse("sample_bin")

    if (frame.schema.columns.indexWhere(columnTuple => columnTuple._1 == outputColumn) >= 0)
      throw new IllegalArgumentException(s"Duplicate column name: ${outputColumn}")

    val seed = arguments.random_seed.getOrElse(0)

    val splitLabels: Array[String] = if (arguments.sample_labels.isEmpty) {
      if (splitPercentages.length == 3) {
        Array("TR", "TE", "VA")
      }
      else {
        (0 to splitPercentages.length - 1).map(i => "Sample#" + i).toArray
      }
    }
    else {
      arguments.sample_labels.get.toArray
    }

    val splitter = new MLDataSplitter(splitPercentages, splitLabels, seed)

    val labeledRDD = splitter.randomlyLabelRDD(frames.getFrameRdd(ctx, frameID))

    val splitRDD = labeledRDD.map(labeledRow => labeledRow.entry :+ labeledRow.label.asInstanceOf[Any])

    splitRDD.saveAsObjectFile(fsRoot + frames.getFrameDataFile(frame.id))

    val allColumns = frame.schema.columns :+ (outputColumn, DataTypes.string)
    frames.updateSchema(frame, allColumns)
  }

  def groupBy(arguments: FrameGroupByColumn[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(groupByCommand, arguments, user, implicitly[ExecutionContext])

  val groupByCommand = commands.registerCommand("dataframe/groupby", groupBySimple)
  def groupBySimple(arguments: FrameGroupByColumn[JsObject, Long], user: UserPrincipal) = {
    implicit val u = user
    val originalFrameID = arguments.frame

    val originalFrame = expectFrame(originalFrameID)

    val ctx = sparkContextManager.context(user).sparkContext
    val schema = originalFrame.schema

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

    val location = fsRoot + frames.getFrameDataFile(newFrame.id)

    val aggregation_arguments = arguments.aggregations

    val args_pair = for {
      (aggregation_function, column_to_apply, new_column_name) <- aggregation_arguments
    } yield (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_to_apply), aggregation_function)

    val new_data_types = if (arguments.group_by_columns.length > 0) {
      val groupByColumns = arguments.group_by_columns

      val columnIndices: Seq[(Int, DataType)] = for {
        col <- groupByColumns
        columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
        columnDataType = schema.columns(columnIndex)._2
      } yield (columnIndex, columnDataType)

      val groupedRDD = frames.getFrameRowRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => {
        for { index <- columnIndices.map(_._1) } yield data(index)
      }.mkString("\0"))
      SparkOps.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, columnIndices.map(_._2).toArray, location)
    }
    else {
      val groupedRDD = frames.getFrameRowRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => "")
      SparkOps.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, Array[DataType](), location)
    }
    val new_column_names = arguments.group_by_columns ++ {
      for { i <- aggregation_arguments } yield i._3
    }
    val new_schema = new_column_names.zip(new_data_types)
    frames.updateSchema(newFrame, new_schema)
  }

  def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    // Python uses different RFC than Java, must correct a couple characters
    // http://stackoverflow.com/questions/21318601/how-to-decode-a-base64-string-in-scala-or-java00
    val corrected = byteStr.map { case '-' => '+'; case '_' => '/'; case c => c }
    new sun.misc.BASE64Decoder().decodeBuffer(corrected)
  }

  /**
   * Create a Python RDD
   * @param frameId source frame for the parent RDD
   * @param py_expression Python expression encoded in Python's Base64 encoding (different than Java's)
   * @param user current user
   * @return the RDD
   */
  private def createPythonRDD(frameId: Long, py_expression: String)(implicit user: UserPrincipal): EnginePythonRDD[String] = {
    withMyClassLoader {
      val ctx = sparkContextManager.context(user).sparkContext
      val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

      val baseRdd: RDD[String] = frames.getFrameRowRdd(ctx, frameId)
        .map(x => x.map(t => t match {
          case null => DataTypes.pythonRddNullString
          case _ => t.toString
        }).mkString(SparkEngine.pythonRddDelimiter))

      val pythonExec = SparkEngineConfig.pythonWorkerExec
      val environment = new java.util.HashMap[String, String]()

      val accumulator = ctx.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())

      val broadcastVars = new JArrayList[Broadcast[Array[Byte]]]()

      val pyRdd = new EnginePythonRDD[String](
        baseRdd, predicateInBytes, environment,
        new JArrayList, preservePartitioning = false,
        pythonExec = pythonExec,
        broadcastVars, accumulator)
      pyRdd
    }
  }

  private def persistPythonRDD(pyRdd: EnginePythonRDD[String], converter: Array[String] => Array[Any], location: String): Unit = {
    withMyClassLoader {
      pyRdd.map(s => new String(s).split(SparkEngine.pythonRddDelimiter)).map(converter).saveAsObjectFile(location)
    }
  }

  /**
   * flatten rdd by the specified column
   * @param arguments input specification for column flattening
   * @param user current user
   */
  override def flattenColumn(arguments: FlattenColumn)(implicit user: UserPrincipal): Execution =
    commands.execute(flattenColumnCommand, arguments, user, implicitly[ExecutionContext])

  val flattenColumnCommand = commands.registerCommand("dataframe/flatten_column", flattenColumnSimple)
  def flattenColumnSimple(arguments: FlattenColumn, user: UserPrincipal) = {
    implicit val u = user
    val frameId: Long = arguments.frameId
    val realFrame = expectFrame(frameId)

    val ctx = sparkContextManager.context(user).sparkContext

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)
    val rdd = frames.getFrameRowRdd(ctx, frameId)

    val columnIndex = realFrame.schema.columnIndex(arguments.column)

    val flattenedRDD = SparkOps.flattenRddByColumnIndex(columnIndex, arguments.separator, rdd)
    val rowCount = flattenedRDD.count()

    flattenedRDD.saveAsObjectFile(fsRoot + frames.getFrameDataFile(newFrame.id))
    frames.updateSchema(newFrame, realFrame.schema.columns)
    frames.updateRowCount(newFrame, rowCount)
  }

  /**
   * Bin the specified column in RDD
   * @param arguments input specification for column binning
   * @param user current user
   */
  override def binColumn(arguments: BinColumn[Long])(implicit user: UserPrincipal): Execution =
    commands.execute(binColumnCommand, arguments, user, implicitly[ExecutionContext])

  val binColumnCommand = commands.registerCommand("dataframe/bin_column", binColumnSimple)
  def binColumnSimple(arguments: BinColumn[Long], user: UserPrincipal) = {
    implicit val u = user
    val frameId: Long = arguments.frame
    val realFrame = expectFrame(frameId)

    val ctx = sparkContextManager.context(user).sparkContext

    val rdd = frames.getFrameRowRdd(ctx, frameId)

    val columnIndex = realFrame.schema.columnIndex(arguments.columnName)

    if (realFrame.schema.columns.indexWhere(columnTuple => columnTuple._1 == arguments.binColumnName) >= 0)
      throw new IllegalArgumentException(s"Duplicate column name: ${arguments.binColumnName}")

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

    arguments.binType match {
      case "equalwidth" => {
        val binnedRdd = SparkOps.binEqualWidth(columnIndex, arguments.numBins, rdd)
        binnedRdd.saveAsObjectFile(fsRoot + frames.getFrameDataFile(newFrame.id))
      }
      case "equaldepth" => {
        val binnedRdd = SparkOps.binEqualDepth(columnIndex, arguments.numBins, rdd)
        binnedRdd.saveAsObjectFile(fsRoot + frames.getFrameDataFile(newFrame.id))
      }
      case _ => throw new IllegalArgumentException(s"Invalid binning type: ${arguments.binType.toString()}")
    }

    val allColumns = realFrame.schema.columns :+ (arguments.binColumnName, DataTypes.int32)
    frames.updateSchema(newFrame, allColumns)
  }

  def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(filterCommand, arguments, user, implicitly[ExecutionContext])

  val filterCommand = commands.registerCommand("dataframe/filter", filterSimple)
  def filterSimple(arguments: FilterPredicate[JsObject, Long], user: UserPrincipal) = {
    implicit val u = user
    val pyRdd = createPythonRDD(arguments.frame, arguments.predicate)
    val rowCount = pyRdd.count()

    val location = fsRoot + frames.getFrameDataFile(arguments.frame)

    val realFrame = frames.lookup(arguments.frame).getOrElse(
      throw new IllegalArgumentException(s"No such data frame: ${arguments.frame}"))
    val schema = realFrame.schema
    val converter = DataTypes.parseMany(schema.columns.map(_._2).toArray)(_)
    persistPythonRDD(pyRdd, converter, location)
    frames.updateRowCount(realFrame, rowCount)
  }

  /**
   * join two data frames
   * @param arguments parameter contains information for the join operation
   * @param user current user
   */
  override def join(arguments: FrameJoin)(implicit user: UserPrincipal): Execution =
    commands.execute(joinCommand, arguments, user, implicitly[ExecutionContext])

  val joinCommand = commands.registerCommand("dataframe/join", joinSimple)
  def joinSimple(arguments: FrameJoin, user: UserPrincipal) = {
    implicit val u = user
    def createPairRddForJoin(arguments: FrameJoin, ctx: SparkContext): List[RDD[(Any, Array[Any])]] = {
      val tupleRddColumnIndex: List[(RDD[Rows.Row], Int)] = arguments.frames.map {
        frame =>
          {
            val realFrame = frames.lookup(frame._1).getOrElse(
              throw new IllegalArgumentException(s"No such data frame"))

            val frameSchema = realFrame.schema
            val rdd = frames.getFrameRowRdd(ctx, frame._1)
            val columnIndex = frameSchema.columnIndex(frame._2)
            (rdd, columnIndex)
          }
      }

      val pairRdds = tupleRddColumnIndex.map {
        t =>
          val rdd = t._1
          val columnIndex = t._2
          rdd.map(p => SparkOps.createKeyValuePairFromRow(p, Seq(columnIndex))).map { case (keyColumns, data) => (keyColumns(0), data) }
      }

      pairRdds
    }

    val originalColumns = arguments.frames.map {
      frame =>
        {
          val realFrame = expectFrame(frame._1)

          realFrame.schema.columns
        }
    }

    val leftColumns: List[(String, DataType)] = originalColumns(0)
    val rightColumns: List[(String, DataType)] = originalColumns(1)
    val allColumns = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)

    /* create a dataframe should take very little time, much less than 10 minutes */
    val newJoinFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

    //first validate join columns are valid
    val leftOn: String = arguments.frames(0)._2
    val rightOn: String = arguments.frames(1)._2

    val leftSchema = Schema(leftColumns)
    val rightSchema = Schema(rightColumns)

    require(leftSchema.columnIndex(leftOn) != -1, s"column $leftOn is invalid")
    require(rightSchema.columnIndex(rightOn) != -1, s"column $rightOn is invalid")

    val ctx = sparkContextManager.context(user).sparkContext
    val pairRdds = createPairRddForJoin(arguments, ctx)

    val joinResultRDD = SparkOps.joinRDDs(RDDJoinParam(pairRdds(0), leftColumns.length),
      RDDJoinParam(pairRdds(1), rightColumns.length),
      arguments.how)
    val joinRowCount = joinResultRDD.count()
    joinResultRDD.saveAsObjectFile(fsRoot + frames.getFrameDataFile(newJoinFrame.id))
    frames.updateSchema(newJoinFrame, allColumns)
    frames.updateRowCount(newJoinFrame, joinRowCount)
  }

  def removeColumn(arguments: FrameRemoveColumn)(implicit user: UserPrincipal): Execution =
    commands.execute(removeColumnCommand, arguments, user, implicitly[ExecutionContext])

  val removeColumnCommand = commands.registerCommand("dataframe/remove_columns", removeColumnSimple)
  def removeColumnSimple(arguments: FrameRemoveColumn, user: UserPrincipal) = {

    implicit val u = user
    val ctx = sparkContextManager.context(user).sparkContext
    val frameId = arguments.frame.id
    val columns = arguments.columns

    val realFrame = expectFrame(arguments.frame)
    val schema = realFrame.schema
    val location = fsRoot + frames.getFrameDataFile(frameId)

    val columnIndices = {
      for {
        col <- columns
        columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
      } yield columnIndex
    }.sorted.distinct

    columnIndices match {
      case invalidColumns if invalidColumns.contains(-1) =>
        throw new IllegalArgumentException(s"Invalid list of columns: [${arguments.columns.mkString(", ")}]")
      case allColumns if allColumns.length == schema.columns.length =>
        frames.getFrameRowRdd(ctx, frameId).filter(_ => false).saveAsObjectFile(location)
      case singleColumn if singleColumn.length == 1 => frames.getFrameRowRdd(ctx, frameId)
        .map(row => row.take(singleColumn(0)) ++ row.drop(singleColumn(0) + 1))
        .saveAsObjectFile(location)
      case multiColumn => frames.getFrameRowRdd(ctx, frameId)
        .map(row => row.zipWithIndex.filter(elem => multiColumn.contains(elem._2) == false).map(_._1))
        .saveAsObjectFile(location)
    }

    frames.removeColumn(realFrame, columnIndices)
  }

  def addColumns(arguments: FrameAddColumns[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(addColumnsCommand, arguments, user, implicitly[ExecutionContext])

  val addColumnsCommand = commands.registerCommand("dataframe/add_columns", addColumnsSimple)
  def addColumnsSimple(arguments: FrameAddColumns[JsObject, Long], user: UserPrincipal) = {
    implicit val u = user
    val ctx = sparkContextManager.context(user).sparkContext
    val frameId = arguments.frame
    val column_names = arguments.column_names
    val column_types = arguments.column_types
    val expression = arguments.expression // Python Wrapper containing lambda expression

    val realFrame = expectFrame(arguments.frame)
    val schema = realFrame.schema
    val location = fsRoot + frames.getFrameDataFile(frameId)

    var newFrame = realFrame
    for {
      i <- 0 until column_names.size
    } {
      val column_name = column_names(i)
      val column_type = column_types(i)
      val columnObject = new BigColumn(column_name)

      if (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_name) >= 0)
        throw new IllegalArgumentException(s"Duplicate column name: $column_name")

      // Update the schema
      newFrame = frames.addColumn(newFrame, columnObject, DataTypes.toDataType(column_type))
    }

    // Update the data
    val pyRdd = createPythonRDD(frameId, expression)
    val converter = DataTypes.parseMany(newFrame.schema.columns.map(_._2).toArray)(_)
    persistPythonRDD(pyRdd, converter, location)
    newFrame
  }

  /**
   * Execute getRows Query plugin
   * @param arguments RowQuery object describing id, offset, and count
   * @param user current user
   * @return the QueryExecution
   */
  def getRowsLarge(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): QueryExecution = {
    queries.execute(getRowsQuery, arguments, user, implicitly[ExecutionContext])
  }
  val getRowsQuery = queries.registerQuery("dataframes/data", getRowsSimple)

  /**
   * Create an intermediate RDD containing the results of a getRows call.
   * This will be used for pagination after completion of the query
   *
   * @param arguments RowQuery object describing id, offset, and count
   * @param user current user
   * @return RDD consisting of the requested number of rows
   */
  def getRowsSimple(arguments: RowQuery[Identifier], user: UserPrincipal) = {
    implicit val impUser: UserPrincipal = user
    val frame = frames.lookup(arguments.id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
    val rows = frames.getRowsRDD(frame, arguments.offset, arguments.count)
    rows
  }

  /**
   * Return a sequence of Rows from an RDD starting from a supplied offset
   *
   * @param arguments RowQuery object describing id, offset, and count
   * @param user current user
   * @return RDD consisting of the requested number of rows
   */
  def getRows(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): Future[Iterable[Row]] = {
    future {
      val frame = frames.lookup(arguments.id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
      val rows = frames.getRows(frame, arguments.offset, arguments.count)
      rows
    }
  }

  def getFrame(id: Identifier)(implicit user: UserPrincipal): Future[Option[DataFrame]] =
    withContext("se.getFrame") {
      future {
        frames.lookup(id)
      }
    }

  /**
   * Register a graph name with the metadata store.
   * @param graph Metadata for graph creation.
   * @param user IMPLICIT. The user creating the graph.
   * @return Future of the graph to be created.
   */
  def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal) = {
    future {
      withMyClassLoader {
        graphs.createGraph(graph)
      }
    }
  }

  /**
   * Loads graph data into a graph in the database. The source is tabular data interpreted by user-specified  rules.
   * @param arguments Graph construction
   * @param user IMPLICIT. The user loading the graph
   * @return Command object for this graphload and a future
   */
  def loadGraph(arguments: GraphLoad)(implicit user: UserPrincipal): Execution =
    commands.execute(loadGraphCommand, arguments, user, implicitly[ExecutionContext])

  val loadGraphCommand = commands.registerCommand("graph/load", loadGraphSimple)
  def loadGraphSimple(arguments: GraphLoad, user: UserPrincipal) = {
    // validating frames
    arguments.frame_rules.foreach(frule => expectFrame(frule.frame))

    val graph = graphs.loadGraph(arguments)(user)
    graph
  }

  /**
   * Obtains a graph's metadata from its identifier.
   * @param id Unique identifier for the graph provided by the metastore.
   * @return A future of the graph metadata entry.
   */
  def getGraph(id: Identifier): Future[Graph] = {
    future {
      graphs.lookup(id).get
    }
  }

  /**
   * Get the metadata for a range of graph identifiers.
   * @param offset First graph to obtain.
   * @param count Number of graphs to obtain.
   * @param user IMPLICIT. User listing the graphs.
   * @return Future of the sequence of graph metadata entries to be returned.
   */
  def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Future[Seq[Graph]] =
    withContext("se.getGraphs") {
      future {
        graphs.getGraphs(offset, count)
      }
    }

  def getGraphByName(name: String)(implicit user: UserPrincipal): Future[Option[Graph]] =
    withContext("se.getGraphByName") {
      future {
        graphs.getGraphByName(name)
      }
    }

  /**
   * Delete a graph from the graph database.
   * @param graph The graph to be deleted.
   * @return A future of unit.
   */
  def deleteGraph(graph: Graph): Future[Unit] = {
    withContext("se.deletegraph") {
      future {
        graphs.drop(graph)
      }
    }
  }

  override def dropDuplicates(arguments: DropDuplicates)(implicit user: UserPrincipal): Execution =
    commands.execute(dropDuplicateCommand, arguments, user, implicitly[ExecutionContext])

  val dropDuplicateCommand = commands.registerCommand("dataframe/drop_duplicates", dropDuplicateSimple)

  def dropDuplicateSimple(dropDuplicateCommand: DropDuplicates, user: UserPrincipal) = {
    implicit val u = user

    val frameId: Long = dropDuplicateCommand.frameId
    val realFrame: DataFrame = getDataFrameById(frameId)

    val ctx = sparkContextManager.context(user).sparkContext

    val frameSchema = realFrame.schema
    val rdd = frames.getFrameRowRdd(ctx, frameId)

    val columnIndices = frameSchema.columnIndex(dropDuplicateCommand.unique_columns)
    val pairRdd = rdd.map(row => SparkOps.createKeyValuePairFromRow(row, columnIndices))

    val duplicatesRemoved: RDD[Array[Any]] = SparkOps.removeDuplicatesByKey(pairRdd)
    val rowCount = duplicatesRemoved.count()

    duplicatesRemoved.saveAsObjectFile(fsRoot + frames.getFrameDataFile(frameId))
    frames.updateRowCount(realFrame, rowCount)
  }

  val calculatePercentileCommand = commands.registerCommand("dataframe/calculate_percentiles", calculatePercentilesSimple)

  def calculatePercentilesSimple(percentiles: CalculatePercentiles, user: UserPrincipal): PercentileValues = {
    implicit val u = user
    val frameId: Long = percentiles.frameId
    val ctx = sparkContextManager.context(user).sparkContext

    val realFrame: DataFrame = getDataFrameById(frameId)
    val frameSchema = realFrame.schema
    val columnIndex = frameSchema.columnIndex(percentiles.columnName)
    val columnDataType = frameSchema.columnDataType(percentiles.columnName)

    val rdd = frames.getFrameRdd(ctx, frameId)
    val percentileValues = SparkOps.calculatePercentiles(rdd, percentiles.percentiles, columnIndex, columnDataType).toList
    PercentileValues(percentileValues)
  }

  override def classificationMetric(arguments: ClassificationMetric[Long])(implicit user: UserPrincipal): Execution =
    commands.execute(classificationMetricCommand, arguments, user, implicitly[ExecutionContext])

  val classificationMetricCommand: CommandPlugin[ClassificationMetric[Long], ClassificationMetricValue] = commands.registerCommand("dataframe/classification_metric", classificationMetricSimple)

  def classificationMetricSimple(arguments: ClassificationMetric[Long], user: UserPrincipal): ClassificationMetricValue = {
    implicit val u = user
    val frameId: Long = arguments.frameId
    val realFrame: DataFrame = getDataFrameById(frameId)

    val ctx = sparkContextManager.context(user).sparkContext

    val frameSchema = realFrame.schema
    val frameRdd = frames.getFrameRowRdd(ctx, frameId)

    val labelColumnIndex = frameSchema.columnIndex(arguments.labelColumn)
    val predColumnIndex = frameSchema.columnIndex(arguments.predColumn)

    val metric_value = arguments.metricType match {
      case "accuracy" => SparkOps.modelAccuracy(frameRdd, labelColumnIndex, predColumnIndex)
      case "precision" => SparkOps.modelPrecision(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
      case "recall" => SparkOps.modelRecall(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
      case "fmeasure" => SparkOps.modelFMeasure(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel, arguments.beta)
      case _ => throw new IllegalArgumentException() // TODO: this exception needs to be handled differently
    }
    ClassificationMetricValue(metric_value)
  }

  override def confusionMatrix(arguments: ConfusionMatrix[Long])(implicit user: UserPrincipal): Execution =
    commands.execute(confusionMatrixCommand, arguments, user, implicitly[ExecutionContext])

  val confusionMatrixCommand: CommandPlugin[ConfusionMatrix[Long], ConfusionMatrixValues] = commands.registerCommand("dataframe/confusion_matrix", confusionMatrixSimple)

  def confusionMatrixSimple(arguments: ConfusionMatrix[Long], user: UserPrincipal): ConfusionMatrixValues = {
    implicit val u = user
    val frameId: Long = arguments.frameId
    val realFrame: DataFrame = getDataFrameById(frameId)(user)

    val ctx = sparkContextManager.context(user).sparkContext

    val frameSchema = realFrame.schema
    val frameRdd = frames.getFrameRdd(ctx, frameId)

    val labelColumnIndex = frameSchema.columnIndex(arguments.labelColumn)
    val predColumnIndex = frameSchema.columnIndex(arguments.predColumn)

    val valueList = SparkOps.confusionMatrix(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)

    ConfusionMatrixValues(valueList)
  }

  /**
   * Retrieve DataFrame object by frame id
   * @param frameId id of the dataframe
   */
  def getDataFrameById(frameId: Long)(implicit user: UserPrincipal): DataFrame = {
    val realFrame = frames.lookup(frameId).getOrElse(
      throw new IllegalArgumentException(s"No such data frame $frameId"))
    realFrame
  }

}
