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

import java.util.{ArrayList => JArrayList, List => JList}

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{DataTypes, SchemaUtil}
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import com.intel.intelanalytics.engine.spark.command.{CommandPluginRegistry, CommandExecutor}
import com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn.BinColumnPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetricsPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist._
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.{GroupByPlugin, GroupByAggregationFunctions}
import com.intel.intelanalytics.engine.spark.frame.plugins.load.{LoadFramePlugin, LoadRDDFunctions}
import com.intel.intelanalytics.engine.spark.frame.plugins._
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.{ColumnMedianPlugin, ColumnModePlugin, ColumnSummaryStatisticsPlugin}
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.quantiles.QuantilesPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.topk.{TopKPlugin, TopKRDDFunctions}
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.graph.plugins.{RenameGraphPlugin, LoadGraphPlugin}
import com.intel.intelanalytics.engine.spark.queries.{SparkQueryStorage, QueryExecutor}
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.NotFoundException
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{EnginePythonAccumulatorParam, EnginePythonRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spray.json._

import DomainJsonProtocol._
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.spark.mllib.util.MLDataSplitter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.ColumnStatistics
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
import com.intel.intelanalytics.domain.query._
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
import com.intel.intelanalytics.domain.frame.ClassificationMetricValue
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.domain.frame.FlattenColumn
import com.intel.intelanalytics.domain.frame.ColumnSummaryStatisticsReturn
import com.intel.intelanalytics.domain.frame.ColumnMedianReturn
import com.intel.intelanalytics.domain.frame.ColumnModeReturn
import com.intel.intelanalytics.domain.frame.FrameJoin
import com.intel.intelanalytics.engine.spark.plugin.{SparkCommandPlugin, SparkInvocation}
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.engine.spark.user.UserStorage
import com.intel.event.EventLogging

object SparkEngine {
  private val pythonRddDelimiter = "YoMeDelimiter"
}

class SparkEngine(sparkContextManager: SparkContextManager,
                  commands: CommandExecutor,
                  commandStorage: CommandStorage,
                  val frames: SparkFrameStorage,
                  val graphs: SparkGraphStorage,
                  users: UserStorage,
                  queryStorage: SparkQueryStorage,
                  queries: QueryExecutor,
                  val sparkAutoPartitioner: SparkAutoPartitioner,
                  commandPluginRegistry: CommandPluginRegistry) extends Engine
with EventLogging
with ClassLoaderAware {

  type Data = FrameRDD
  type Context = SparkContext

  val fsRoot = SparkEngineConfig.fsRoot
  override val pageSize: Int = SparkEngineConfig.pageSize

  commandPluginRegistry.registerCommand(new LoadFramePlugin)
  // Registering plugins
  Seq(new LoadFramePlugin,
    new RenameFramePlugin,
    new RenameColumnsPlugin,
    new ProjectPlugin,
    new AssignSamplePlugin,
    new GroupByPlugin,
    new FlattenColumnPlugin,
    new BinColumnPlugin,
    new ColumnModePlugin,
    new ColumnMedianPlugin,
    new ColumnSummaryStatisticsPlugin,
    new FilterPlugin,
    new JoinPlugin(frames),
    new DropColumnsPlugin,
    new AddColumnsPlugin,
    new DropDuplicatesPlugin,
    new QuantilesPlugin,
    new ClassificationMetricsPlugin,
    new EcdfPlugin,
    new TallyPercentPlugin,
    new TallyPlugin,
    new CumulativePercentPlugin,
    new CumulativeSumPlugin,
    new ShannonEntropyPlugin,
    new TopKPlugin,
    new LoadGraphPlugin,
    new RenameGraphPlugin).foreach {
      (c: SparkCommandPlugin[_ <: Product,_ <: Product]) => commandPluginRegistry.registerCommand(c)
  }

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
   * return a query object
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

  override def getUserPrincipal(apiKey: String): UserPrincipal = {
    users.getUserPrincipal(apiKey)
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
    pluginRegistry.registerCommand("frame:/column_full_statistics", columnFullStatisticSimple)

  def columnFullStatisticSimple(arguments: ColumnFullStatistics, user: UserPrincipal): ColumnFullStatisticsReturn = {

    implicit val u = user

    val frameId: Long = arguments.frame.id
    val frame = frames.expectFrame(frameId)
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

  val getRowsQuery = queries.registerQuery("frames/data", getRowsSimple)

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
      val rdd = frames.loadLegacyFrameRdd(invocation.sparkContext, arguments.id).rows
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
   * @return A QueryResult describing the data and schema of this take
   */
  def getRows(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): QueryResult = {
    withMyClassLoader {
      val frame = frames.lookup(arguments.id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
      if (frames.isParquet(frame)) {
        val rows = frames.getRows(frame, arguments.offset, arguments.count)
        QueryDataResult(rows, Some(frame.schema))
      }
      else {
        getRowsLarge(arguments)
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

  override def cancelCommand(id: Long)(implicit user: UserPrincipal): Future[Unit] = withContext("se.cancelCommand") {
    future {
      commands.stopCommand(id)
    }
  }

  override def shutdown(): Unit = {
    //do nothing
  }

}
