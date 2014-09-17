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
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, SchemaUtil }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import com.intel.intelanalytics.engine.spark.command.{ CommandPluginRegistry, CommandExecutor }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.queries.{ SparkQueryStorage, QueryExecutor }
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.NotFoundException
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ EnginePythonAccumulatorParam, EnginePythonRDD }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spray.json._

import DomainJsonProtocol._
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.spark.mllib.util.MLDataSplitter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.intel.intelanalytics.engine.spark.statistics.{ TopKRDDFunctions, EntropyRDDFunctions, ColumnStatistics }
import org.apache.spark.engine.SparkProgressListener
import com.intel.intelanalytics.domain.frame.Entropy
import com.intel.intelanalytics.domain.frame.EntropyReturn
import com.intel.intelanalytics.domain.frame.TopK
import com.intel.intelanalytics.domain.frame.FrameAddColumns
import com.intel.intelanalytics.domain.frame.RenameFrame
import com.intel.intelanalytics.domain.graph.RenameGraph
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.domain.frame.DropDuplicates
import com.intel.intelanalytics.domain.frame.FrameProject
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.frame.load.Load
import com.intel.intelanalytics.domain.frame.CumulativeSum
import com.intel.intelanalytics.domain.frame.CumulativeCount
import com.intel.intelanalytics.domain.frame.CumulativePercentCount
import com.intel.intelanalytics.domain.frame.CumulativePercentSum
import com.intel.intelanalytics.domain.frame.Quantiles
import com.intel.intelanalytics.domain.frame.AssignSample
import com.intel.intelanalytics.domain.frame.FrameGroupByColumn
import com.intel.intelanalytics.domain.frame.FrameRenameColumns
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.FrameDropColumns
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.engine.spark.frame.RDDJoinParam
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.query.Query
import com.intel.intelanalytics.domain.frame.ColumnSummaryStatistics
import com.intel.intelanalytics.domain.frame.ColumnMedian
import com.intel.intelanalytics.domain.frame.ColumnMode
import com.intel.intelanalytics.domain.frame.ECDF
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.engine.ProgressInfo
import com.intel.intelanalytics.domain.command.CommandDefinition
import com.intel.intelanalytics.domain.frame.ClassificationMetric
import com.intel.intelanalytics.domain.frame.BinColumn
import com.intel.intelanalytics.domain.frame.QuantileValues
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.command.Execution
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.query.RowQuery
import com.intel.intelanalytics.domain.frame.ClassificationMetricValue
//import com.intel.intelanalytics.domain.frame.ConfusionMatrixValues
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.domain.frame.FlattenColumn
import com.intel.intelanalytics.domain.frame.ColumnSummaryStatisticsReturn
import com.intel.intelanalytics.domain.frame.ColumnMedianReturn
import com.intel.intelanalytics.domain.frame.ColumnModeReturn
import com.intel.intelanalytics.domain.frame.FrameJoin
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.query.PagedQueryResult
import com.intel.intelanalytics.domain.query.QueryDataResult

object SparkEngine {
  private val pythonRddDelimiter = "YoMeDelimiter"
}

class SparkEngine(sparkContextManager: SparkContextManager,
                  commands: CommandExecutor,
                  commandStorage: CommandStorage,
                  frames: SparkFrameStorage,
                  graphs: SparkGraphStorage,
                  queryStorage: SparkQueryStorage,
                  queries: QueryExecutor,
                  sparkAutoPartitioner: SparkAutoPartitioner,
                  commandPluginRegistry: CommandPluginRegistry) extends Engine
    with EventLogging
    with ClassLoaderAware {

  private val fsRoot = SparkEngineConfig.fsRoot
  override val pageSize: Int = SparkEngineConfig.pageSize

  /* This progress listener saves progress update to command table */
  SparkProgressListener.progressUpdater = new CommandProgressUpdater {

    var lastUpdateTime = System.currentTimeMillis()
    /**
     * save the progress update
     * @param commandId id of the command
     * @param progressInfo list of progress for jobs initiated by the command
     */
    override def updateProgress(commandId: Long, progressInfo: List[ProgressInfo]): Unit = {
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastUpdateTime > 1000) {
        lastUpdateTime = currentTime
        commandStorage.updateProgress(commandId, progressInfo)
      }
    }
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
    withMyClassLoader {
      val ctx = sparkContextManager.context(user, "query")
      try {
        val data = queryStorage.getQueryPage(ctx, id, pageId)
        com.intel.intelanalytics.domain.query.QueryDataResult(data, None)
      }
      finally {
        ctx.stop()
      }
    }
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
    commands.execute(command, user, implicitly[ExecutionContext], commandPluginRegistry)

  /**
   * All the command definitions available
   */
  override def getCommandDefinitions()(implicit user: UserPrincipal): Iterable[CommandDefinition] = {
    commandPluginRegistry.getCommandDefinitions()
  }

  def load(arguments: Load)(implicit user: UserPrincipal): Execution =
    commands.execute(loadCommand, arguments, user, implicitly[ExecutionContext])

  val loadCommand = commandPluginRegistry.registerCommand("dataframe/load", loadSimple _, numberOfJobs = 8)

  /**
   * Load data from a LoadSource object to an existing destination described in the Load object
   * @param load Load command object
   * @param user current user
   */
  def loadSimple(load: Load, user: UserPrincipal, invocation: SparkInvocation): DataFrame = {
    val frameId = load.destination.id
    val destinationFrame = expectFrame(frameId)
    val ctx = invocation.sparkContext

    if (load.source.isFrame) {
      // load data from an existing frame and add its data onto the target frame
      val additionalData = frames.loadFrameRdd(ctx, expectFrame(load.source.uri.toInt))
      unionAndSave(ctx, destinationFrame, additionalData)
    }
    else if (load.source.isFile) {
      val parser = load.source.parser.get
      val partitions = sparkAutoPartitioner.partitionsForFile(load.source.uri)
      val parseResult = LoadRDDFunctions.loadAndParseLines(ctx, fsRoot + "/" + load.source.uri, parser, partitions)

      // parse failures go to their own data frame
      if (parseResult.errorLines.count() > 0) {
        val errorFrame = frames.lookupOrCreateErrorFrame(destinationFrame)
        unionAndSave(ctx, errorFrame, parseResult.errorLines)
      }

      // successfully parsed lines get added to the destination frame
      unionAndSave(ctx, destinationFrame, parseResult.parsedLines)
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
    val existingRdd = frames.loadFrameRdd(sparkContext, existingFrame)
    val unionedRdd = existingRdd.union(additionalData)
    val rowCount = unionedRdd.count()
    frames.saveFrame(existingFrame, unionedRdd, Some(rowCount))
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

  def getFrames()(implicit p: UserPrincipal): Future[Seq[DataFrame]] = withContext("se.getFrames") {
    future {
      frames.getFrames()
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

  def renameFrame(arguments: RenameFrame)(implicit user: UserPrincipal): Execution =
    commands.execute(renameFrameCommand, arguments, user, implicitly[ExecutionContext])

  val renameFrameCommand = commandPluginRegistry.registerCommand("dataframe/rename_frame", renameFrameSimple _)

  private def renameFrameSimple(arguments: RenameFrame, user: UserPrincipal, invocation: SparkInvocation): DataFrame = {
    val frame = expectFrame(arguments.frame)
    val newName = arguments.newName
    frames.renameFrame(frame, newName)
  }

  def renameColumns(arguments: FrameRenameColumns[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(renameColumnsCommand, arguments, user, implicitly[ExecutionContext])

  val renameColumnsCommand = commandPluginRegistry.registerCommand("dataframe/rename_columns", renameColumnsSimple _)
  def renameColumnsSimple(arguments: FrameRenameColumns[JsObject, Long], user: UserPrincipal, invocation: SparkInvocation) = {
    val frameID = arguments.frame
    val realFrame = expectFrame(frameID)
    val newNames = arguments.new_names
    val schema = realFrame.schema
    val originalNames = arguments.original_names

    for {
      i <- 0 until newNames.size
    } {
      val column_name = newNames(i)
      val original_name = originalNames(i)
      if (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_name) >= 0)
        throw new IllegalArgumentException(s"Cannot rename because another column already exists with that name: $column_name")

      if (schema.columns.indexWhere(columnTuple => columnTuple._1 == original_name) < 0)
        throw new IllegalArgumentException(s"Cannot rename because there is no column with that name: $original_name")
    }

    /**
     * for cn in column_names:
     * present = False
     * for current_name in current_names:
     * if cn == current_name:
     * present = True
     * if not present:
     * raise ValueError ("Cannot rename because column name '{0}' is not present in current column names".format(cn))
     */

    frames.renameColumns(realFrame, arguments.original_names.zip(arguments.new_names))
  }

  def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(projectCommand, arguments, user, implicitly[ExecutionContext])

  val projectCommand = commandPluginRegistry.registerCommand("dataframe/project", projectSimple _)
  def projectSimple(arguments: FrameProject[JsObject, Long], user: UserPrincipal, invocation: SparkInvocation): DataFrame = {

    implicit val u = user

    val sourceFrameID = arguments.frame
    val sourceFrame = expectFrame(sourceFrameID)
    val projectedFrameID = arguments.projected_frame
    val projectedFrame = expectFrame(projectedFrameID)
    val ctx = invocation.sparkContext
    val columns = arguments.columns

    val schema = sourceFrame.schema

    val columnIndices = for {
      col <- columns
      columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
    } yield columnIndex

    if (columnIndices.contains(-1)) {
      throw new IllegalArgumentException(s"Invalid list of columns: ${arguments.columns.toString()}")
    }

    val resultRdd = frames.loadFrameRdd(ctx, sourceFrameID)
      .map(row => {
        for { i <- columnIndices } yield row(i)
      }.toArray)

    val projectedColumns = arguments.new_column_names match {
      case empty if empty.size == 0 => for { i <- columnIndices } yield schema.columns(i)
      case _ =>
        for { i <- 0 until columnIndices.size }
          yield (arguments.new_column_names(i), schema.columns(columnIndices(i))._2)
    }

    frames.saveFrame(projectedFrame, new FrameRDD(new Schema(projectedColumns.toList), resultRdd), Some(sourceFrame.rowCount))
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

  val assignSampleDoc = CommandDoc(oneLineSummary = "Assign classes to rows.",
    extendedSummary = Some("""
    Randomly assign classes to rows given a vector of percentages.
    The table receives an additional column that contains a random label generated by the probability distribution
    function specified by a list of floating point values.
    The labels are non-negative integers drawn from the range [ 0,  len(split_percentages) - 1].
    Optionally, the user can specify a list of strings to be used as the labels. If the number of labels is 3,
    the labels will default to "TR", "TE" and "VA".

    Parameters
    ----------
    sample_percentages : list of floating point values
        Entries are non-negative and sum to 1.
        If the *i*'th entry of the  list is *p*,
        then then each row receives label *i* with independent probability *p*.
    sample_labels : str (optional)
        Names to be used for the split classes.
        Defaults "TR", "TE", "VA" when there are three numbers given in split_percentages,
        defaults to Sample#0, Sample#1, ... otherwise.
    output_column : str (optional)
        Name of the new column which holds the labels generated by the function
    random_seed : int (optional)
        Random seed used to generate the labels. Defaults to 0.

    Examples
    --------
    For this example, my_frame is a BigFrame object accessing a frame with data.
    Append a new column *sample_bin* to the frame;
    Assign the value in the new column to "train", "test", or "validate"::

        my_frame.assign_sample([0.3, 0.3, 0.4], ["train", "test", "validate"])

    Now the frame accessed by BigFrame *my_frame* has a new column named "sample_bin" and each row contains one of the values "train",
    "test", or "validate".  Values in the other columns are unaffected.
    """))

  val assignSampleCommand = commandPluginRegistry.registerCommand("dataframe/assign_sample", assignSampleSimple _, doc = Some(assignSampleDoc))

  def assignSampleSimple(arguments: AssignSample, user: UserPrincipal, invocation: SparkInvocation) = {

    val ctx = invocation.sparkContext

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

    val labeledRDD = splitter.randomlyLabelRDD(frames.loadFrameRdd(ctx, frameID))

    val splitRDD = labeledRDD.map(labeledRow => labeledRow.entry :+ labeledRow.label.asInstanceOf[Any])

    frames.saveFrameWithoutSchema(frame, splitRDD)

    val allColumns = frame.schema.columns :+ (outputColumn, DataTypes.string)
    frames.updateSchema(frame, allColumns)
  }

  def groupBy(arguments: FrameGroupByColumn[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(groupByCommand, arguments, user, implicitly[ExecutionContext])

  val groupByCommand = commandPluginRegistry.registerCommand("dataframe/group_by", groupBySimple _)
  def groupBySimple(arguments: FrameGroupByColumn[JsObject, Long], user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val originalFrameID = arguments.frame

    val originalFrame = expectFrame(originalFrameID)

    val ctx = invocation.sparkContext
    val schema = originalFrame.schema

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

    val aggregation_arguments = arguments.aggregations

    val args_pair = for {
      (aggregation_function, column_to_apply, new_column_name) <- aggregation_arguments
    } yield (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_to_apply), aggregation_function)

    if (arguments.group_by_columns.length > 0) {
      val groupByColumns = arguments.group_by_columns

      val columnIndices: Seq[(Int, DataType)] = for {
        col <- groupByColumns
        columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
        columnDataType = schema.columns(columnIndex)._2
      } yield (columnIndex, columnDataType)

      val groupedRDD = frames.loadFrameRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => {
        for { index <- columnIndices.map(_._1) } yield data(index)
      }.mkString("\0"))
      val resultRdd = SparkOps.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, columnIndices.map(_._2).toArray, arguments)
      frames.saveFrame(newFrame, resultRdd)
    }
    else {
      val groupedRDD = frames.loadFrameRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => "")
      val resultRdd = SparkOps.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, Array[DataType](), arguments)
      frames.saveFrame(newFrame, resultRdd)
    }
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
  private def createPythonRDD(frameId: Long, py_expression: String, ctx: SparkContext)(implicit user: UserPrincipal): EnginePythonRDD[String] = {
    withMyClassLoader {
      val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

      val baseRdd: RDD[String] = frames.loadFrameRdd(ctx, frameId)
        .map(x => x.map(t => t match {
          case null => JsNull
          case a => a.toJson
        }).toJson.toString)

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

  /**
   * Persists a PythonRDD after python computation is complete to HDFS
   *
   * @param dataFrame DataFrame associated with this RDD
   * @param pyRdd PythonRDD instance
   * @param converter Schema Function converter to convert internals of RDD from Array[String] to Array[Any]
   * @param skipRowCount Skip counting rows when persisting RDD for optimizing speed
   * @return rowCount Number of rows if skipRowCount is false, else 0 (for optimization/transformations which do not alter row count)
   */
  private def persistPythonRDD(dataFrame: DataFrame, pyRdd: EnginePythonRDD[String], converter: Array[String] => Array[Any], skipRowCount: Boolean = false): Long = {
    withMyClassLoader {

      val resultRdd = pyRdd.map(s => JsonParser(new String(s)).convertTo[List[List[JsValue]]].map(y => y.map(x => x match {
        case x if x.isInstanceOf[JsString] => x.asInstanceOf[JsString].value
        case x if x.isInstanceOf[JsNumber] => x.asInstanceOf[JsNumber].toString
        case x if x.isInstanceOf[JsBoolean] => x.asInstanceOf[JsBoolean].toString
        case _ => null
      }).toArray))
        .flatMap(identity)
        .map(converter)

      val rowCount = if (skipRowCount) 0 else resultRdd.count()
      frames.saveFrameWithoutSchema(dataFrame, resultRdd)
      rowCount
    }
  }

  /**
   * flatten rdd by the specified column
   * @param arguments input specification for column flattening
   * @param user current user
   */
  override def flattenColumn(arguments: FlattenColumn)(implicit user: UserPrincipal): Execution =
    commands.execute(flattenColumnCommand, arguments, user, implicitly[ExecutionContext])

  val flattenColumnCommand = commandPluginRegistry.registerCommand("dataframe/flatten_column", flattenColumnSimple _)
  def flattenColumnSimple(arguments: FlattenColumn, user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId: Long = arguments.frameId.id
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)
    val rdd = frames.loadFrameRdd(ctx, frameId)

    val columnIndex = realFrame.schema.columnIndex(arguments.column)

    val flattenedRDD = SparkOps.flattenRddByColumnIndex(columnIndex, arguments.separator, rdd)
    val rowCount = flattenedRDD.count()

    frames.saveFrame(newFrame, new FrameRDD(realFrame.schema, flattenedRDD))
    frames.updateRowCount(newFrame, rowCount)
  }

  /**
   * Bin the specified column in RDD
   * @param arguments input specification for column binning
   * @param user current user
   */
  override def binColumn(arguments: BinColumn[Long])(implicit user: UserPrincipal): Execution =
    commands.execute(binColumnCommand, arguments, user, implicitly[ExecutionContext])

  val binColumnCommand = commandPluginRegistry.registerCommand("dataframe/bin_column", binColumnSimple _, numberOfJobs = 7)
  def binColumnSimple(arguments: BinColumn[Long], user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId: Long = arguments.frame
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val rdd = frames.loadFrameRdd(ctx, frameId)

    val columnIndex = realFrame.schema.columnIndex(arguments.columnName)

    if (realFrame.schema.columns.indexWhere(columnTuple => columnTuple._1 == arguments.binColumnName) >= 0)
      throw new IllegalArgumentException(s"Duplicate column name: ${arguments.binColumnName}")

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

    val allColumns = realFrame.schema.columns :+ (arguments.binColumnName, DataTypes.int32)

    arguments.binType match {
      case "equalwidth" => {
        val binnedRdd = SparkOps.binEqualWidth(columnIndex, arguments.numBins, rdd)
        frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), binnedRdd))
      }
      case "equaldepth" => {
        val binnedRdd = SparkOps.binEqualDepth(columnIndex, arguments.numBins, rdd)
        frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), binnedRdd))
      }
      case _ => throw new IllegalArgumentException(s"Invalid binning type: ${arguments.binType.toString()}")
    }

    frames.updateSchema(newFrame, allColumns)
  }

  /**
   * Calculate the mode of the specified column.
   * @param arguments Input specification for column mode.
   * @param user Current user.
   */
  override def columnMode(arguments: ColumnMode)(implicit user: UserPrincipal): Execution =
    commands.execute(columnModeCommand, arguments, user, implicitly[ExecutionContext])

  val columnModeDoc = CommandDoc(oneLineSummary = "Calculate modes of a column.",
    extendedSummary = Some("""
    Calculate modes of a column.  A mode is a data element of maximum weight. All data elements of weight <= 0
    are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight > 0, no mode is returned.

    Because data distributions often have mutliple modes, it is possible for a set of modes to be returned. By
    default, only one is returned, but my setting the optional parameter max_number_of_modes_returned, a larger
    number of modes can be returned.

    Parameters
    ----------
    data_column : str
        The column whose mode is to be calculated

    weights_column : str
        Optional. The column that provides weights (frequencies) for the mode calculation.
        Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
        parameter is not provided.

    max_modes_returned : int
        Optional. Maximum number of modes returned. If this parameter is not provided, it defaults to 1

    Returns
    -------
    mode : Dict
        Dictionary containing summary statistics in the following entries:
            mode : A mode is a data element of maximum net weight. A set of modes is returned.
             The empty set is returned when the sum of the weights is 0. If the number of modes is <= the parameter
             maxNumberOfModesReturned, then all modes of the data are returned.If the number of modes is
             > maxNumberOfModesReturned, then only the first maxNumberOfModesReturned many modes
             (per a canonical ordering) are returned.
            weight_of_mode : Weight of a mode. If there are no data elements of finite weight > 0,
             the weight of the mode is 0. If no weights column is given, this is the number of appearances of
             each mode.
            total_weight : Sum of all weights in the weight column. This is the row count if no weights
             are given. If no weights column is given, this is the number of rows in the table with non-zero weight.
            mode_count : The number of distinct modes in the data. In the case that the data is very multimodal,
             this number may well exceed max_number_of_modes_returned.

    Example
    -------
    >>> mode = frame.column_mode('modum columpne')
"""))

  val columnModeCommand: CommandPlugin[ColumnMode, ColumnModeReturn] =
    commandPluginRegistry.registerCommand("dataframe/column_mode", columnModeSimple _, doc = Some(columnModeDoc))

  def columnModeSimple(arguments: ColumnMode, user: UserPrincipal, invocation: SparkInvocation): ColumnModeReturn = {

    implicit val u = user

    val frameId = arguments.frame.id
    val frame = expectFrame(frameId)
    val ctx = invocation.sparkContext
    val rdd = frames.loadFrameRdd(ctx, frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2

    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }

    val modeCountOption = arguments.maxModesReturned

    ColumnStatistics.columnMode(columnIndex,
      valueDataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      modeCountOption,
      rdd)
  }

  /**
   * Calculate the median of the specified column.
   * param arguments Input specification for column median.
   * param user Current user.
   *
   */

  override def columnMedian(arguments: ColumnMedian)(implicit user: UserPrincipal): Execution =
    commands.execute(columnMedianCommand, arguments, user, implicitly[ExecutionContext])

  val columnMedianDoc = CommandDoc(oneLineSummary = "Calculate (weighted) median of a column.",
    extendedSummary = Some("""
                             |Calculate the (weighted) median of a column. The median is the least value X in the range of the distribution so
                             |         that the cumulative weight of values strictly below X is strictly less than half of the total weight and
                             |          the cumulative weight of values up to and including X is >= 1/2 the total weight.
                             |
                             |        All data elements of weight <= 0 are excluded from the calculation, as are all data elements whose weight
                             |         is NaN or infinite. If a weight column is provided and no weights are finite numbers > 0, None is returned.
                             |
                             |        Parameters
                             |        ----------
                             |        data_column : str
                             |            The column whose median is to be calculated.
                             |
                             |        weights_column : str
                             |            Optional. The column that provides weights (frequencies) for the median calculation.
                             |            Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
                             |                parameter is not provided.
                             |
                             |        Returns
                             |        -------
                             |        median :  The median of the values.  If a weight column is provided and no weights are finite numbers > 0,
                             |             None is returned. Type of the median returned is that of the contents of the data column, so a column of
                             |             Longs will result in a Long median and a column of Floats will result in a Float median.
                             |
                             |        Example
                             |        -------
                             |        >>> median = frame.column_median('middling column')
                             |""".stripMargin))

  val columnMedianCommand: CommandPlugin[ColumnMedian, ColumnMedianReturn] =
    commandPluginRegistry.registerCommand("dataframe/column_median", columnMedianSimple, doc = Some(columnMedianDoc))

  def columnMedianSimple(arguments: ColumnMedian, user: UserPrincipal, invocation: SparkInvocation): ColumnMedianReturn = {

    implicit val u = user

    val frameId: Long = arguments.frame.id
    val frame = expectFrame(frameId)
    val ctx = invocation.sparkContext
    val rdd = frames.loadFrameRdd(ctx, frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2

    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }

    ColumnStatistics.columnMedian(columnIndex, valueDataType, weightsColumnIndexOption, weightsDataTypeOption, rdd)
  }

  /**
   * Calculate summary statistics of the specified column.
   * @param arguments Input specification for column summary statistics.
   * @param user Current user.
   */
  override def columnSummaryStatistics(arguments: ColumnSummaryStatistics)(implicit user: UserPrincipal): Execution =
    commands.execute(columnStatisticCommand, arguments, user, implicitly[ExecutionContext])

  val columnSummaryStatisticsDoc = CommandDoc(oneLineSummary = "Calculate summary statistics of a column.",
    extendedSummary = Some("""
                             |        Calculate summary statistics of a column.
                             |
                             |        Parameters
                             |        ----------
                             |        data_column : str
                             |            The column to be statistically summarized.
                             |            Must contain numerical data; all NaNs and infinite values are excluded from the calculation.
                             |        weights_column_name : str (optional)
                             |            Name of column holding weights of column values
                             |        use_population_variance : bool (optional)
                             |            If true, the variance is calculated as the population variance. If false, the variance calculated as the
                             |             sample variance. Because this option affects the variance, it affects the standard deviation and the
                             |             confidence intervals as well. This option is False by default, so that sample variance is the default
                             |             form of variance calculated.
                             |        Returns
                             |        -------
                             |        summary : Dict
                             |            Dictionary containing summary statistics in the following entries:
                             |
                             |            | mean:
                             |                  Arithmetic mean of the data.
                             |
                             |            | geometric_mean:
                             |                  Geometric mean of the data. None when there is a data element <= 0, 1.0 when there are no data
                             |                  elements.
                             |
                             |            | variance:
                             |                  None when there are <= 1 many data elements.
                             |                  Sample variance is the weighted sum of the squared distance of each data element from the
                             |                   weighted mean, divided by the total weight minus 1. None when the sum of the weights is <= 1.
                             |                  Population variance is the weighted sum of the squared distance of each data element from the
                             |                   weighted mean, divided by the total weight.
                             |
                             |            | standard_deviation:
                             |                  The square root of the variance. None when  sample variance
                             |                  is being used and the sum of weights is <= 1.
                             |
                             |
                             |            | valid_data_count:
                             |                  The count of all data elements that are finite numbers.
                             |                  (In other words, after excluding NaNs and infinite values.)
                             |
                             |            | minimum:
                             |                  Minimum value in the data. None when there are no data elements.
                             |
                             |            | maximum:
                             |                  Maximum value in the data. None when there are no data elements.
                             |
                             |            | mean_confidence_lower:
                             |                  Lower limit of the 95% confidence interval about the mean.
                             |                  Assumes a Gaussian distribution.
                             |                  None when there are no elements of positive weight.
                             |
                             |            | mean_confidence_upper:
                             |                  Upper limit of the 95% confidence interval about the mean.
                             |                  Assumes a Gaussian distribution.
                             |                  None when there are no elements of positive weight.
                             |
                             |            | bad_row_count : The number of rows containing a NaN or infinite value in either the data
                             |                  or weights column.
                             |
                             |            | good_row_count : The number of rows not containing a NaN or infinite value in either the data
                             |                  or weights column.
                             |
                             |            | positive_weight_count : The number of valid data elements with weight > 0.
                             |                   This is the number of entries used in the statistical calculation.
                             |
                             |            | non_positive_weight_count : The number valid data elements with finite weight <= 0.
                             |
                             |        Notes
                             |        -----
                             |        Return Types
                             |            | valid_data_count returns a Long.
                             |            | All other values are returned as Doubles or None.
                             |
                             |        Sample Variance
                             |            Sample Variance is computed by the following formula:
                             |
                             |        .. math::
                             |
                             |            \\left( \\frac{1}{W - 1} \\right) * sum_{i}  \\left(x_{i} - M \\right) ^{2}
                             |
                             |        where :math:`W` is sum of weights over valid elements of positive weight, and :math:`M` is the weighted mean.
                             |
                             |        Population Variance
                             |            Population Variance is computed by the following formula:
                             |
                             |        .. math::
                             |
                             |            \\left( \\frac{1}{W} \\right) * sum_{i}  \\left(x_{i} - M \\right) ^{2}
                             |
                             |        where :math:`W` is sum of weights over valid elements of positive weight, and :math:`M` is the weighted mean.
                             |
                             |        Standard Deviation
                             |            The square root of the variance.
                             |
                             |        Logging Invalid Data
                             |        --------------------
                             |
                             |        A row is bad when it contains a NaN or infinite value in either its data or weights column.  In this case, it
                             |         contributes to bad_row_count; otherwise it contributes to good row count.
                             |
                             |        A good row can be skipped because the value in its weight column is <=0. In this case, it contributes to
                             |         non_positive_weight_count, otherwise (when the weight is > 0) it contributes to valid_data_weight_pair_count.
                             |
                             |            Equations
                             |            ---------
                             |            bad_row_count + good_row_count = # rows in the frame
                             |            positive_weight_count + non_positive_weight_count = good_row_count
                             |
                             |        In particular, when no weights column is provided and all weights are 1.0, non_positive_weight_count = 0 and
                             |         positive_weight_count = good_row_count
                             |
                             |        Examples
                             |        --------
                             |        ::
                             |
                             |            stats = frame.column_summary_statistics('data column', 'weight column')
                             |
                             |        .. versionadded:: 0.8
    |""".stripMargin))

  val columnStatisticCommand: CommandPlugin[ColumnSummaryStatistics, ColumnSummaryStatisticsReturn] =
    commandPluginRegistry.registerCommand("dataframe/column_summary_statistics", columnStatisticSimple _, doc = Some(columnSummaryStatisticsDoc))

  def columnStatisticSimple(arguments: ColumnSummaryStatistics, user: UserPrincipal, invocation: SparkInvocation): ColumnSummaryStatisticsReturn = {

    implicit val u = user

    val frameId: Long = arguments.frame.id
    val frame = expectFrame(frameId)
    val ctx = invocation.sparkContext
    val rdd = frames.loadFrameRdd(ctx, frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2

    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }

    val usePopulationVariance = arguments.usePopulationVariance.getOrElse(false)

    ColumnStatistics.columnSummaryStatistics(columnIndex,
      valueDataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      rdd,
      usePopulationVariance)
  }

  // TODO TRIB-2245
  /*
  /**
   * Calculate full statistics of the specified column.
   * @param arguments Input specification for column statistics.
   * @param user Current user.
   */
  override def columnFullStatistics(arguments: ColumnFullStatistics)(implicit user: UserPrincipal): Execution =
    commands.execute(columnFullStatisticsCommand, arguments, user, implicitly[ExecutionContext])

  val columnFullStatisticsCommand: CommandPlugin[ColumnFullStatistics, ColumnFullStatisticsReturn] =
    pluginRegistry.registerCommand("dataframe/column_full_statistics", columnFullStatisticSimple)

  def columnFullStatisticSimple(arguments: ColumnFullStatistics, user: UserPrincipal): ColumnFullStatisticsReturn = {

    implicit val u = user

    val frameId: Long = arguments.frame.id
    val frame = expectFrame(frameId)
    val ctx = sparkContextManager.context(user).sparkContext
    val rdd = frames.getFrameRdd(ctx, frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2

    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }

    ColumnStatistics.columnFullStatistics(columnIndex, valueDataType, weightsColumnIndexOption, weightsDataTypeOption, rdd)
  }
 */

  def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(filterCommand, arguments, user, implicitly[ExecutionContext])

  val filterCommand = commandPluginRegistry.registerCommand("dataframe/filter", filterSimple _, numberOfJobs = 2)
  def filterSimple(arguments: FilterPredicate[JsObject, Long], user: UserPrincipal, invocation: SparkInvocation): DataFrame = {
    implicit val u = user
    val pyRdd = createPythonRDD(arguments.frame, arguments.predicate, invocation.sparkContext)

    val realFrame = frames.lookup(arguments.frame).getOrElse(
      throw new IllegalArgumentException(s"No such data frame: ${arguments.frame}"))
    val schema = realFrame.schema
    val converter = DataTypes.parseMany(schema.columns.map(_._2).toArray)(_)
    val rowCount = persistPythonRDD(realFrame, pyRdd, converter, skipRowCount = false)
    frames.updateRowCount(realFrame, rowCount)
  }

  /**
   * join two data frames
   * @param arguments parameter contains information for the join operation
   * @param user current user
   */
  override def join(arguments: FrameJoin)(implicit user: UserPrincipal): Execution =
    commands.execute(joinCommand, arguments, user, implicitly[ExecutionContext])

  val joinCommand = commandPluginRegistry.registerCommand("dataframe/join", joinSimple _)
  def joinSimple(arguments: FrameJoin, user: UserPrincipal, invocation: SparkInvocation): DataFrame = {
    implicit val u = user
    def createPairRddForJoin(arguments: FrameJoin, ctx: SparkContext): List[RDD[(Any, Array[Any])]] = {
      val tupleRddColumnIndex: List[(RDD[Rows.Row], Int)] = arguments.frames.map {
        frame =>
          {
            val realFrame = frames.lookup(frame._1).getOrElse(
              throw new IllegalArgumentException(s"No such data frame"))

            val frameSchema = realFrame.schema
            val rdd = frames.loadFrameRdd(ctx, frame._1)
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

    val ctx = invocation.sparkContext
    val pairRdds = createPairRddForJoin(arguments, ctx)

    val joinResultRDD = SparkOps.joinRDDs(RDDJoinParam(pairRdds(0), leftColumns.length),
      RDDJoinParam(pairRdds(1), rightColumns.length),
      arguments.how)

    val joinRowCount = joinResultRDD.count()
    frames.saveFrame(newJoinFrame, new FrameRDD(new Schema(allColumns), joinResultRDD), Some(joinRowCount))
  }

  def dropColumns(arguments: FrameDropColumns)(implicit user: UserPrincipal): Execution =
    commands.execute(dropColumnsCommand, arguments, user, implicitly[ExecutionContext])

  val dropColumnsDoc = CommandDoc(oneLineSummary = "Remove columns from the frame.",
    extendedSummary = Some("""
    Remove columns from the frame.  They are deleted.

    Parameters
    ----------
    columns: str OR list of str
        column name OR list of column names to be removed from the frame

    Notes
    -----
    Deleting the last column in a frame leaves the frame empty.

    Examples
    --------
    For this example, BigFrame object * my_frame * accesses a frame with columns * column_a *, * column_b *, * column_c * and * column_d *.
    Eliminate columns * column_b * and * column_d *::
    my_frame.drop_columns([column_b, column_d])
    Now the frame only has the columns * column_a * and * column_c *.
    For further examples, see: ref: `example_frame.drop_columns`"""))
  val dropColumnsCommand = commandPluginRegistry.registerCommand("dataframe/drop_columns", dropColumnsSimple _, doc = Some(dropColumnsDoc))
  def dropColumnsSimple(arguments: FrameDropColumns, user: UserPrincipal, invocation: SparkInvocation): DataFrame = {

    implicit val u = user
    val ctx = invocation.sparkContext
    val frameId = arguments.frame.id
    val columns = arguments.columns

    val realFrame = expectFrame(arguments.frame)
    val schema = realFrame.schema

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
        val resultRdd = frames.loadFrameRdd(ctx, frameId).filter(_ => false)
        frames.saveFrameWithoutSchema(realFrame, resultRdd)
      case singleColumn if singleColumn.length == 1 =>
        val resultRdd = frames.loadFrameRdd(ctx, realFrame)
          .map(row => row.take(singleColumn(0)) ++ row.drop(singleColumn(0) + 1))
        frames.saveFrameWithoutSchema(realFrame, resultRdd)
      case multiColumn =>
        val resultRdd = frames.loadFrameRdd(ctx, frameId)
          .map(row => row.zipWithIndex.filter(elem => multiColumn.contains(elem._2) == false).map(_._1))
        frames.saveFrameWithoutSchema(realFrame, resultRdd)
    }

    frames.dropColumns(realFrame, columnIndices)
  }

  def addColumns(arguments: FrameAddColumns[JsObject, Long])(implicit user: UserPrincipal): Execution =
    commands.execute(addColumnsCommand, arguments, user, implicitly[ExecutionContext])

  val addColumnsCommand = commandPluginRegistry.registerCommand("dataframe/add_columns", addColumnsSimple _)
  def addColumnsSimple(arguments: FrameAddColumns[JsObject, Long], user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val ctx = invocation.sparkContext
    val frameId = arguments.frame
    val column_names = arguments.column_names
    val column_types = arguments.column_types
    val expression = arguments.expression // Python Wrapper containing lambda expression

    val realFrame = expectFrame(arguments.frame)
    val schema = realFrame.schema

    var newColumns = schema.columns
    for {
      i <- 0 until column_names.size
    } {
      val column_name = column_names(i)
      val column_type = column_types(i)

      if (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_name) >= 0)
        throw new IllegalArgumentException(s"Duplicate column name: $column_name")

      // Update the schema
      newColumns = newColumns :+ (column_name, DataTypes.toDataType(column_type))
    }

    // Update the data
    val pyRdd = createPythonRDD(frameId, expression, invocation.sparkContext)
    val converter = DataTypes.parseMany(newColumns.map(_._2).toArray)(_)
    persistPythonRDD(realFrame, pyRdd, converter, skipRowCount = true)
    frames.updateSchema(realFrame, newColumns)
  }

  /**
   * Execute getRows Query plugin
   * @param arguments RowQuery object describing id, offset, and count
   * @param user current user
   * @return the QueryExecution
   */
  def getRowsLarge(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): PagedQueryResult = {
    val queryExecution = queries.execute(getRowsQuery, arguments, user, implicitly[ExecutionContext])
    val frame = frames.lookup(arguments.id).get
    val schema = frame.schema
    PagedQueryResult(queryExecution, Some(schema))
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
  def getRowsSimple(arguments: RowQuery[Identifier], user: UserPrincipal, invocation: SparkInvocation) = {
    if (arguments.count + arguments.offset <= SparkEngineConfig.pageSize) {
      val rdd = frames.loadFrameRdd(invocation.sparkContext, arguments.id).rows
      val takenRows = rdd.take(arguments.count + arguments.offset.toInt).drop(arguments.offset.toInt)
      invocation.sparkContext.parallelize(takenRows)
    }
    else {
      implicit val impUser: UserPrincipal = user
      val frame = frames.lookup(arguments.id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
      val rows = frames.getPagedRowsRDD(frame, arguments.offset, arguments.count, invocation.sparkContext)
      rows
    }
  }

  /**
   * Return a sequence of Rows from an RDD starting from a supplied offset
   *
   * @param arguments RowQuery object describing id, offset, and count
   * @param user current user
   * @return RDD consisting of the requested number of rows
   */
  def getRows(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): Future[QueryDataResult] = {
    future {
      withMyClassLoader {
        val frame = frames.lookup(arguments.id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
        val ctx = sparkContextManager.context(user, "query")
        try {
          val rdd: RDD[Row] = frames.loadFrameRdd(ctx, frame).rows
          val rows = rdd.take(arguments.count + arguments.offset.toInt).drop(arguments.offset.toInt)
          QueryDataResult(rows, Some(frame.schema))
        }
        finally {
          ctx.stop()
        }
      }
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

  val loadGraphCommand = commandPluginRegistry.registerCommand("graph/load", loadGraphSimple _, numberOfJobs = 2)
  def loadGraphSimple(arguments: GraphLoad, user: UserPrincipal, invocation: SparkInvocation) = {
    //validating frames    
    arguments.frame_rules.foreach(frule => expectFrame(frule.frame))

    val graph = graphs.loadGraph(arguments, invocation)(user)
    graph
  }

  /**
   * Renames a graph in the database
   * @param rename RenameGraph object storing the graph and the newName
   * @param user IMPLICIT. The user loading the graph
   * @return Graph object
   */

  def renameGraph(rename: RenameGraph)(implicit user: UserPrincipal): Execution =
    commands.execute(renameGraphCommand, rename, user, implicitly[ExecutionContext])

  val renameGraphCommand = commandPluginRegistry.registerCommand("graph/rename_graph", renameGraphSimple)
  def renameGraphSimple(rename: RenameGraph, user: UserPrincipal, invocation: SparkInvocation): Graph = {
    val graphId = rename.graph.id
    val graph = graphs.lookup(graphId).getOrElse(throw new NotFoundException("graph", graphId.toString))
    val newName = rename.newName
    graphs.renameGraph(graph, newName)
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
   * @param user IMPLICIT. User listing the graphs.
   * @return Future of the sequence of graph metadata entries to be returned.
   */
  def getGraphs()(implicit user: UserPrincipal): Future[Seq[Graph]] =
    withContext("se.getGraphs") {
      future {
        graphs.getGraphs()
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

  val dropDuplicateCommand = commandPluginRegistry.registerCommand("dataframe/drop_duplicates", dropDuplicateSimple _, numberOfJobs = 2)

  def dropDuplicateSimple(dropDuplicateCommand: DropDuplicates, user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user

    val frameId: Long = dropDuplicateCommand.frameId
    val realFrame: DataFrame = getDataFrameById(frameId)

    val ctx = invocation.sparkContext

    val frameSchema = realFrame.schema
    val rdd = frames.loadFrameRdd(ctx, frameId)

    val columnIndices = frameSchema.columnIndex(dropDuplicateCommand.unique_columns)
    val pairRdd = rdd.map(row => SparkOps.createKeyValuePairFromRow(row, columnIndices))

    val duplicatesRemoved: RDD[Array[Any]] = SparkOps.removeDuplicatesByKey(pairRdd)
    val rowCount = duplicatesRemoved.count()

    frames.saveFrameWithoutSchema(realFrame, duplicatesRemoved)
    frames.updateRowCount(realFrame, rowCount)
  }

  val quantilesCommand = commandPluginRegistry.registerCommand("dataframe/quantiles", quantilesSimple _, numberOfJobs = 7)

  def quantilesSimple(quantiles: Quantiles, user: UserPrincipal, invocation: SparkInvocation): QuantileValues = {
    implicit val u = user
    val frameId: Long = quantiles.frameId
    val ctx = invocation.sparkContext

    val realFrame: DataFrame = getDataFrameById(frameId)
    val frameSchema = realFrame.schema
    val columnIndex = frameSchema.columnIndex(quantiles.columnName)
    val columnDataType = frameSchema.columnDataType(quantiles.columnName)

    val rdd = frames.loadFrameRdd(ctx, frameId)
    val quantileValues = SparkOps.quantiles(rdd, quantiles.quantiles, columnIndex, columnDataType).toList
    QuantileValues(quantileValues)
  }

  override def classificationMetrics(arguments: ClassificationMetric)(implicit user: UserPrincipal): Execution =
    commands.execute(classificationMetricsCommand, arguments, user, implicitly[ExecutionContext])
  val classificationMetricsDoc = CommandDoc(oneLineSummary = "Computes Model accuracy, precision, recall, confusion matrix and f_measure (math:`F_{\\beta}`).",
    extendedSummary = Some("""
    Based on the *metric_type* argument provided, it computes the accuracy, precision, recall or :math:`F_{\\beta}` measure for a classification model

    --- When metric_type provided is 'f_measure': Computes the :math:`F_{\\beta}` measure for a classification model.
    A column containing the correct labels for each instance and a column containing the predictions made by the model are specified.
    The :math:`F_{\\beta}` measure of a binary classification model is the harmonic mean of precision and recall.
    If we let:

    * beta :math:`\\equiv \\beta`,
    * :math:`T_{P}` denote the number of true positives,
    * :math:`F_{P}` denote the number of false positives, and
    * :math:`F_{N}` denote the number of false negatives,

    then:
    .. math::
      F_{\\beta} = \\left(1 + \\beta ^ 2\\right) * \\frac{\\frac{T_{P}}{T_{P} + F_{P}} * \\frac{T_{P}}{T_{P} + F_{N}}}{\\beta ^ 2 * \\
      \\left(\\frac{T_{P}}{T_{P} + F_{P}} + \\frac{T_{P}}{T_{P} + F_{N}}\\right)}

    For multi-class classification, the :math:`F_{\\beta}` measure is computed as the weighted average of the :math:`F_{\\beta}` measure
    for each label, where the weight is the number of instance with each label in the labeled column.  The
    determination of binary vs. multi-class is automatically inferred from the data.

    --- When metric_type provided is 'recall': Computes the recall measure for a classification model.
    A column containing the correct labels for each instance and a column containing the predictions made by the model are specified.
    The recall of a binary classification model is the proportion of positive instances that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives and :math:`F_{N}` denote the number of false
    negatives, then the model recall is given by: :math:`\\frac {T_{P}} {T_{P} + F_{N}}`.

    For multi-class classification, the recall measure is computed as the weighted average of the recall
    for each label, where the weight is the number of instance with each label in the labeled column.  The
    determination of binary vs. multi-class is automatically inferred from the data.

    --- When metric_type provided is 'precision': Computes the precision measure for a classification model
    A column containing the correct labels for each instance and a column containing the predictions made by the
    model are specified.  The precision of a binary classification model is the proportion of predicted positive
    instances that are correct.  If we let :math:`T_{P}` denote the number of true positives and :math:`F_{P}` denote the number of false
    positives, then the model precision is given by: :math:`\\frac {T_{P}} {T_{P} + F_{P}}`.

    For multi-class classification, the precision measure is computed as the weighted average of the precision
    for each label, where the weight is the number of instances with each label in the labeled column.  The
    determination of binary vs. multi-class is automatically inferred from the data.

    --- When metric_type provided is 'accuracy': Computes the accuracy measure for a classification model
    A column containing the correct labels for each instance and a column containing the predictions made by the classifier are specified.
    The accuracy of a classification model is the proportion of predictions that are correct.
    If we let :math:`T_{P}` denote the number of true positives, :math:`T_{N}` denote the number of true negatives, and :math:`K`
    denote the total number of classified instances, then the model accuracy is given by: :math:`\\frac{T_{P} + T_{N}}{K}`.

    This measure applies to binary and multi-class classifiers.


    Parameters
    ----------
    metric_type : str
      the model that is to be computed
    label_column : str
      the name of the column containing the correct label for each instance
    pred_column : str
      the name of the column containing the predicted label for each instance
    pos_label : str
      the value to be interpreted as a positive instance (only for binary, ignored for multi-class)
    beta : float
      beta value to use for :math:`F_{\\beta}` measure (default F1 measure is computed); must be greater than zero

    Returns
    -------
    float64
    the measure for the classifier

    Examples
    --------
    Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
    the predicted labels in the *predictions* column::

    frame.inspect()

    a:unicode   b:int32   labels:int32  predictions:int32
    |-------------------------------------------------------|
    red               1              0                  0
    blue              3              1                  0
    blue              1              0                  0
    green             0              1                  1

    frame.classification_metrics('f_measure', 'labels', 'predictions', '1', 1)

    0.66666666666666663

    frame.classification_metrics('f_measure', 'labels', 'predictions', '1', 2)

    0.55555555555555558

    frame.classification_metrics('f_measure', 'labels', 'predictions', '0', 1)

    0.80000000000000004


    frame.classification_metrics('recall', 'labels', 'predictions', '1', 1)

    0.5

    frame.classification_metrics('recall', 'labels', 'predictions', '0', 1)

    1.0


    frame.classification_metrics('precision', 'labels', 'predictions', '1', 1)

    1.0

    frame.classification_metrics('precision', 'labels', 'predictions', '0', 1)

    0.66666666666666663


    frame.classification_metrics('accuracy', 'labels', 'predictions', '1', 1)

    0.75

    frame.classification_metrics('confusion_matrix', 'labels', 'predictions', '1', 1)

    [1, 2, 0, 1]


    .. versionadded:: 0.8  """))
  val classificationMetricsCommand: CommandPlugin[ClassificationMetric, ClassificationMetricValue] = commandPluginRegistry.registerCommand("dataframe/classification_metrics", classificationMetricsSimple _, doc = Some(classificationMetricsDoc))

  def classificationMetricsSimple(arguments: ClassificationMetric, user: UserPrincipal, invocation: SparkInvocation): ClassificationMetricValue = {
    implicit val u = user
    val frameId = arguments.frame.id
    val realFrame: DataFrame = getDataFrameById(frameId)

    val ctx = invocation.sparkContext

    val frameSchema = realFrame.schema
    val frameRdd = frames.loadFrameRdd(ctx, frameId)

    val labelColumnIndex = frameSchema.columnIndex(arguments.labelColumn)
    val predColumnIndex = frameSchema.columnIndex(arguments.predColumn)

    if (arguments.metricType == "confusion_matrix") {
      val valueList = SparkOps.confusionMatrix(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
      ClassificationMetricValue(None, Some(valueList))
    }
    else {
      val metric_value = arguments.metricType match {
        case "accuracy" => SparkOps.modelAccuracy(frameRdd, labelColumnIndex, predColumnIndex)
        case "precision" => SparkOps.modelPrecision(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
        case "recall" => SparkOps.modelRecall(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
        case "f_measure" => SparkOps.modelFMeasure(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel, arguments.beta)
        case _ => throw new IllegalArgumentException() // TODO: this exception needs to be handled differently
      }
      ClassificationMetricValue(Some(metric_value), None)
    }

  }

  override def ecdf(arguments: ECDF[Long])(implicit user: UserPrincipal): Execution =
    commands.execute(ecdfCommand, arguments, user, implicitly[ExecutionContext])

  val ecdfCommand = commandPluginRegistry.registerCommand("dataframe/ecdf", ecdfSimple _)

  def ecdfSimple(arguments: ECDF[Long], user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId: Long = arguments.frameId
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val rdd = frames.loadFrameRdd(ctx, frameId)

    val sampleIndex = realFrame.schema.columnIndex(arguments.sampleCol)

    val newFrame = Await.result(create(DataFrameTemplate(arguments.name, None)), SparkEngineConfig.defaultTimeout)

    val ecdfRdd = SparkOps.ecdf(rdd, sampleIndex, arguments.dataType)

    val columnName = "_ECDF"
    val allColumns = arguments.dataType match {
      case "int32" => List((arguments.sampleCol, DataTypes.int32), (arguments.sampleCol + columnName, DataTypes.float64))
      case "int64" => List((arguments.sampleCol, DataTypes.int64), (arguments.sampleCol + columnName, DataTypes.float64))
      case "float32" => List((arguments.sampleCol, DataTypes.float32), (arguments.sampleCol + columnName, DataTypes.float64))
      case "float64" => List((arguments.sampleCol, DataTypes.float64), (arguments.sampleCol + columnName, DataTypes.float64))
      case _ => List((arguments.sampleCol, DataTypes.string), (arguments.sampleCol + columnName, DataTypes.float64))
    }

    frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), ecdfRdd))

    newFrame.copy(schema = Schema(allColumns))
  }

  def tallyPercent(arguments: CumulativePercentCount)(implicit user: UserPrincipal): Execution =
    commands.execute(cumulativePercentCountCommand, arguments, user, implicitly[ExecutionContext])
  val tallyPercentDoc = CommandDoc(oneLineSummary = "Computes a cumulative percent count.",
    extendedSummary = Some("""
                             |Compute a cumulative percent count.
                             |
                             |        A cumulative percent count is computed by sequentially stepping through the column values and keeping track of
                             |        the current percentage of the total number of times the specified *count_value* has been seen up to the current
                             |        value.
                             |
                             |        Parameters
                             |        ----------
                             |        sample_col : string
                             |            The name of the column from which to compute the cumulative sum
                             |        count_value : string
                             |            The column value to be used for the counts
                             |
                             |        Returns
                             |        -------
                             |        BigFrame
                             |            A new object accessing a new frame containing the original columns appended with a column containing the cumulative percent counts
                             |
                             |        Examples
                             |        --------
                             |        Consider BigFrame *my_frame*, which accesses a frame that contains a single column named *obs*::
                             |
                             |            my_frame.inspect()
                             |
                             |             obs int32
                             |            |---------|
                             |               0
                             |               1
                             |               2
                             |               0
                             |               1
                             |               2
                             |
                             |        The cumulative percent count for column *obs* is obtained by::
                             |
                             |            cpc_frame = my_frame.tally_percent('obs', 1)
                             |
                             |        The BigFrame *cpc_frame* accesses a new frame that contains two columns, *obs* that contains the original column values, and
                             |        *obsCumulativePercentCount* that contains the cumulative percent count::
                             |
                             |            cpc_frame.inspect()
                             |
                             |             obs int32   obsCumulativePercentCount float64
                             |            |---------------------------------------------|
                             |               0                          0.0
                             |               1                          0.5
                             |               2                          0.5
                             |               0                          0.5
                             |               1                          1.0
                             |               2                          1.0
                             |
                             |        .. versionadded:: 0.8 """.stripMargin))
  val cumulativePercentCountCommand = commandPluginRegistry.registerCommand("dataframe/tally_percent", cumulativePercentCountSimple _, doc = Some(tallyPercentDoc))
  def cumulativePercentCountSimple(arguments: CumulativePercentCount, user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId = arguments.frame.id
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val frameRdd = frames.loadFrameRdd(ctx, frameId)

    val sampleIndex = realFrame.schema.columnIndex(arguments.sampleCol)

    val newFrame = Await.result(create(DataFrameTemplate(realFrame.name, None)), SparkEngineConfig.defaultTimeout)

    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativePercentCount(frameRdd, sampleIndex, arguments.countVal), "_cumulative_percent_count")

    val frameSchema = realFrame.schema
    val allColumns = frameSchema.columns :+ (arguments.sampleCol + columnName, DataTypes.float64)

    frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), cumulativeDistRdd))

    newFrame.copy(schema = Schema(allColumns))
  }

  override def tally(arguments: CumulativeCount)(implicit user: UserPrincipal): Execution =
    commands.execute(cumulativeCountCommand, arguments, user, implicitly[ExecutionContext])
  val tallyDoc = CommandDoc(oneLineSummary = "Computes a cumulative count.",
    extendedSummary = Some("""
        Compute a cumulative count.

        A cumulative count is computed by sequentially stepping through the column values and keeping track of the
        the number of times the specified *count_value* has been seen up to the current value.

        Parameters
        ----------
        sample_col : string
            The name of the column from which to compute the cumulative count
        count_value : string
            The column value to be used for the counts

        Returns
        -------
        BigFrame
            A new object accessing a new frame containing the original columns appended with a column containing the cumulative counts

        Examples
        --------
        Consider BigFrame *my_frame*, which accesses a frame that contains a single column *obs*::

            my_frame.inspect()

             obs int32
             |---------|
               0
               1
               2
               0
               1
               2

        The cumulative count for column *obs* using *count_value = 1* is obtained by::

            cc_frame = my_frame.tally('obs', '1')

        The BigFrame *cc_frame* accesses a frame which contains two columns *obs* and *obsCumulativeCount*.
        Column *obs* still has the same data and *obsCumulativeCount* contains the cumulative counts::

            cc_frame.inspect()

             obs int32   obsCumulativeCount int32
             |------------------------------------|
               0                          0
               1                          1
               2                          1
               0                          1
               1                          2
               2                          2

        .. versionadded:: 0.8 """))
  val cumulativeCountCommand = commandPluginRegistry.registerCommand("dataframe/tally", cumulativeCountSimple _, doc = Some(tallyDoc))
  def cumulativeCountSimple(arguments: CumulativeCount, user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId = arguments.frame.id
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val frameRdd = frames.loadFrameRdd(ctx, frameId)

    val sampleIndex = realFrame.schema.columnIndex(arguments.sampleCol)

    val newFrame = Await.result(create(DataFrameTemplate(realFrame.name, None)), SparkEngineConfig.defaultTimeout)

    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativeCount(frameRdd, sampleIndex, arguments.countVal), "_cumulative_count")

    val frameSchema = realFrame.schema
    val allColumns = frameSchema.columns :+ (arguments.sampleCol + columnName, DataTypes.float64)

    frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), cumulativeDistRdd))

    newFrame.copy(schema = Schema(allColumns))
  }

  def cumPercent(arguments: CumulativePercentSum)(implicit user: UserPrincipal): Execution =
    commands.execute(cumulativePercentSumCommand, arguments, user, implicitly[ExecutionContext])
  val cumPercentDoc = CommandDoc(oneLineSummary = "Computes a cumulative percent sum.",
    extendedSummary = Some("""
    Compute a cumulative percent sum.

    A cumulative percent sum is computed by sequentially stepping through the column values and keeping track of the
    current percentage of the total sum accounted
    for at the current value.

    Parameters
    ----------
    sample_col: string
      The name of the column from which to compute the cumulative percent sum

    Returns
    -------
    BigFrame
      A new object accessing a new frame containing the original columns appended with a column containing the cumulative percent sums

    Notes
    -----
      This function applies only to columns containing numerical data.

    Examples
    --------
    Consider BigFrame * my_frame * accessing a frame that contains a single column named * obs *::

        my_frame.inspect()

        obs int32
        |---------|
          0
          1
          2
          0
          1
          2

    The cumulative percent sum for column * obs * is obtained by ::

    cps_frame = my_frame.cumulative_percent('obs')

    The new frame accessed by BigFrame * cps_frame * contains two columns * obs * and * obsCumulativePercentSum *.
    They contain the original data and the cumulative percent sum, respectively ::

        cps_frame.inspect()

        obs int32 obsCumulativePercentSum float64
        |-------------------------------------------|
          0 0.0
          1 0.16666666
          2 0.5
          0 0.5
          1 0.66666666
          2 1.0

      ..versionadded :: 0.8 """))
  val cumulativePercentSumCommand = commandPluginRegistry.registerCommand("dataframe/cumulative_percent", cumulativePercentSumSimple _, doc = Some(cumPercentDoc))
  def cumulativePercentSumSimple(arguments: CumulativePercentSum, user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId = arguments.frame.id
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val frameRdd = frames.loadFrameRdd(ctx, frameId)

    val sampleIndex = realFrame.schema.columnIndex(arguments.sampleCol)

    val newFrame = Await.result(create(DataFrameTemplate(realFrame.name, None)), SparkEngineConfig.defaultTimeout)

    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativePercentSum(frameRdd, sampleIndex), "_cumulative_percent_sum")

    val frameSchema = realFrame.schema
    val allColumns = frameSchema.columns :+ (arguments.sampleCol + columnName, DataTypes.float64)

    frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), cumulativeDistRdd))

    newFrame.copy(schema = Schema(allColumns))
  }

  def cumSum(arguments: CumulativeSum)(implicit user: UserPrincipal): Execution =
    commands.execute(cumulativeSumCommand, arguments, user, implicitly[ExecutionContext])
  val cumSumDoc = CommandDoc(oneLineSummary = "Computes a cumulative sum.",
    extendedSummary = Some("""
        Compute a cumulative sum.

        A cumulative sum is computed by sequentially stepping through the column values and keeping track of the current
        cumulative sum for each value.

        Parameters
        ----------
        sample_col : string
            The name of the column from which to compute the cumulative sum

        Returns
        -------
        BigFrame
            A new object accessing a frame containing the original columns appended with a column containing the cumulative sums

        Notes
        -----
        This function applies only to columns containing numerical data.

        Examples
        --------
        Consider BigFrame *my_frame*, which accesses a frame that contains a single column named *obs*::

             my_frame.inspect()

             obs int32
             |---------|
               0
               1
               2
               0
               1
               2

        The cumulative sum for column *obs* is obtained by::

            cs_frame = my_frame.cumulative_sum('obs')

        The BigFrame *cs_frame* accesses a new frame that contains two columns, *obs* that contains the original column values, and
        *obsCumulativeSum* that contains the cumulative percent count::

            cs_frame.inspect()

             obs int32   obsCumulativeSum int32
             |----------------------------------|
               0                     0
               1                     1
               2                     3
               0                     3
               1                     4
               2                     6

        .. versionadded:: 0.8 """))
  val cumulativeSumCommand = commandPluginRegistry.registerCommand("dataframe/cumulative_sum", cumulativeSumSimple _, doc = Some(cumSumDoc))
  def cumulativeSumSimple(arguments: CumulativeSum, user: UserPrincipal, invocation: SparkInvocation) = {
    implicit val u = user
    val frameId = arguments.frame.id
    val realFrame = expectFrame(frameId)

    val ctx = invocation.sparkContext

    val frameRdd = frames.loadFrameRdd(ctx, frameId)

    val sampleIndex = realFrame.schema.columnIndex(arguments.sampleCol)

    val newFrame = Await.result(create(DataFrameTemplate(realFrame.name, None)), SparkEngineConfig.defaultTimeout)

    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativeSum(frameRdd, sampleIndex), "_cumulative_sum")

    val frameSchema = realFrame.schema
    val allColumns = frameSchema.columns :+ (arguments.sampleCol + columnName, DataTypes.float64)

    frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), cumulativeDistRdd))

    newFrame.copy(schema = Schema(allColumns))
  }

  override def cancelCommand(id: Long)(implicit user: UserPrincipal): Future[Unit] = withContext("se.cancelCommand") {
    future {
      commands.stopCommand(id)
    }
  }

  /**
   * Calculate the entropy of the specified column.
   *
   * @param arguments Input specification for column entropy
   * @param user Current user
   */
  override def entropy(arguments: Entropy)(implicit user: UserPrincipal): Execution =
    commands.execute(entropyCommand, arguments, user, implicitly[ExecutionContext])

  val entropyDoc = CommandDoc(oneLineSummary = "Calculate Shannon entropy of a column.",
    extendedSummary = Some("""
    Calculate the Shannon entropy of a column. The column can be weighted. All data elements of weight <= 0
    are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight > 0, the entropy is zero.

    Parameters
    ----------
    data_column : str
        The column whose entropy is to be calculated

    weights_column : str (Optional)
        The column that provides weights (frequencies) for the entropy calculation.
        Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
        parameter is not provided.

    Returns
    -------
    entropy : float64

    Example
    -------
    >>> entropy = frame.shannon_entropy('data column')
    >>> weighted_entropy = frame.shannon_entropy('data column', 'weight column')

    ..versionadded :: 0.8 """))

  val entropyCommand = commandPluginRegistry.registerCommand("dataframe/shannon_entropy",
    entropyCommandSimple _, numberOfJobs = 3, doc = Some(entropyDoc))

  def entropyCommandSimple(arguments: Entropy, user: UserPrincipal, invocation: SparkInvocation): EntropyReturn = {
    implicit val u = user

    val frameRef = arguments.frame
    val frame = expectFrame(frameRef)
    val ctx = invocation.sparkContext
    val frameRdd = frames.loadFrameRdd(ctx, frameRef.id)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val (weightsColumnIndexOption, weightsDataTypeOption) = getColumnIndexAndType(frame, arguments.weightsColumn)

    val entropy = EntropyRDDFunctions.shannonEntropy(frameRdd, columnIndex, weightsColumnIndexOption, weightsDataTypeOption)
    EntropyReturn(entropy)
  }

  /**
   * Calculate the top (or bottom) K distinct values by count for specified data column.
   *
   * @param arguments Input specification for topK command
   * @param user Current user
   */
  override def topK(arguments: TopK)(implicit user: UserPrincipal): Execution =
    commands.execute(topKCommand, arguments, user, implicitly[ExecutionContext])

  val topKDoc = CommandDoc(oneLineSummary = "Calculate the top (or bottom) K distinct values by count of a column.",
    extendedSummary = Some("""
    Calculate the top (or bottom) K distinct values by count of a column. The column can be weighted.
    All data elements of weight <= 0 are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight > 0, then topK is empty.

    Parameters
    ----------
    data_column : str
        The column whose top (or bottom) K distinct values are to be calculated

    k : int
        Number of entries to return (If k is negative, return bottom k)

    weights_column : str (Optional)
        The column that provides weights (frequencies) for the topK calculation.
        Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
        parameter is not provided.

    Returns
    -------

    BigFrame : An object with access to the frame

    Example
    -------
    For this example, we calculate the top 5 movie genres in a data frame.

     >>> top5 = frame.top_k('genre', 5)
     >>> top5.inspect()

      genre:str   count:float64
      ----------------------------
      Drama        738278
      Comedy       671398
      Short        455728
      Documentary  323150
      Talk-Show    265180

   This example calculates the top 3 movies weighted by rating.

     >>> top3 = frame.top_k('genre', 3, weights_column='rating')
     >>> top3.inspect()

     movie:str   count:float64
     -----------------------------
     The Godfather         7689.0
     Shawshank Redemption  6358.0
     The Dark Knight       5426.0

   This example calculates the bottom 3 movie genres in a data frame.

    >>> bottom3 = frame.top_k('genre', -3)
    >>> bottom3.inspect()

       genre:str   count:float64
       ----------------------------
       Musical      26
       War          47
       Film-Noir    595

    ..versionadded :: 0.8 """))

  val topKCommand =
    commandPluginRegistry.registerCommand("dataframe/top_k", topKCommandSimple _, numberOfJobs = 3, doc = Some(topKDoc))

  def topKCommandSimple(arguments: TopK, user: UserPrincipal, invocation: SparkInvocation): DataFrame = {
    implicit val u = user

    val frameId = arguments.frame
    val frame = expectFrame(frameId)
    val ctx = invocation.sparkContext
    val frameRdd = frames.loadFrameRdd(ctx, frameId.id)
    val columnIndex = frame.schema.columnIndex(arguments.columnName)
    val valueDataType = frame.schema.columns(columnIndex)._2
    val (weightsColumnIndexOption, weightsDataTypeOption) = getColumnIndexAndType(frame, arguments.weightsColumn)

    val newFrameName = frames.generateFrameName()

    val newFrame = Await.result(create(DataFrameTemplate(newFrameName, None)), SparkEngineConfig.defaultTimeout)
    val useBottomK = arguments.k < 0
    val topRdd = TopKRDDFunctions.topK(frameRdd, columnIndex, Math.abs(arguments.k), useBottomK,
      weightsColumnIndexOption, weightsDataTypeOption)

    val newSchema = Schema(List(
      (arguments.columnName, valueDataType),
      ("count", DataTypes.float64)
    ))

    val rowCount = topRdd.count()
    frames.saveFrame(newFrame, new FrameRDD(newSchema, topRdd), Some(rowCount))
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

  override def shutdown(): Unit = {
    //do nothing
  }

  /**
   * Get column index and data type of a column in a data frame.
   *
   * @param frame Data frame
   * @param columnName Column name
   * @return Option with the column index and data type
   */
  private def getColumnIndexAndType(frame: DataFrame, columnName: Option[String]): (Option[Int], Option[DataType]) = {

    val (columnIndexOption, dataTypeOption) = columnName match {
      case Some(columnIndex) => {
        val weightsColumnIndex = frame.schema.columnIndex(columnIndex)
        (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
      }
      case None => (None, None)
    }
    (columnIndexOption, dataTypeOption)
  }
}
