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

import com.intel.event.{ EventContext, EventLogging }
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.command.{ Command, CommandDefinition, CommandTemplate, Execution }
import com.intel.intelanalytics.domain.frame.{ DataFrame, DataFrameTemplate }
import com.intel.intelanalytics.domain.graph.{ Graph, GraphTemplate }
import com.intel.intelanalytics.domain.model.{ Model, ModelTemplate }
import com.intel.intelanalytics.domain.query._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, CommandPluginRegistry }
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.engine.spark.frame.plugins._
import com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn.BinColumnPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetricsPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist._
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.{ GroupByPlugin, GroupByAggregationFunctions }
import com.intel.intelanalytics.engine.spark.frame.plugins.load.{ LoadFramePlugin, LoadRDDFunctions }
import com.intel.intelanalytics.engine.spark.frame.plugins._
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.{ ColumnMedianPlugin, ColumnModePlugin, ColumnSummaryStatisticsPlugin }
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.multivariatestatistics.CovariancePlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.quantiles.QuantilesPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance.CovarianceMatrixPlugin
import com.intel.intelanalytics.engine.spark.frame.plugins.topk.TopKPlugin
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.graph.plugins._
import com.intel.intelanalytics.engine.spark.queries.{ SparkQueryStorage, QueryExecutor }
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.{ EventLoggingImplicits, NotFoundException }
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ EnginePythonAccumulatorParam, EnginePythonRDD }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.ia.plugins.{ LogisticRegressionWithSGDTrainPlugin, LogisticRegressionWithSGDTestPlugin }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.graph.plugins.{ LoadGraphPlugin, RenameGraphPlugin }
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.engine.spark.queries.{ QueryExecutor, SparkQueryStorage }
import com.intel.intelanalytics.engine.spark.user.UserStorage
import com.intel.intelanalytics.engine.{ ProgressInfo, _ }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.engine.SparkProgressListener
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import spray.json._

import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
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
import com.intel.intelanalytics.domain.frame.CovarianceMatrixArguments
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
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.engine.spark.user.UserStorage

object SparkEngine {
  private val pythonRddDelimiter = "YoMeDelimiter"
}

class SparkEngine(sparkContextFactory: SparkContextFactory,
                  commands: CommandExecutor,
                  commandStorage: CommandStorage,
                  val frames: SparkFrameStorage,
                  val graphs: SparkGraphStorage,
                  val models: SparkModelStorage,
                  users: UserStorage,
                  queryStorage: SparkQueryStorage,
                  queries: QueryExecutor,
                  val sparkAutoPartitioner: SparkAutoPartitioner,
                  commandPluginRegistry: CommandPluginRegistry) extends Engine
    with EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  type Data = FrameRDD
  type Context = SparkContext

  val fsRoot = SparkEngineConfig.fsRoot
  override val pageSize: Int = SparkEngineConfig.pageSize

  // Registering frame plugins
  commandPluginRegistry.registerCommand(new LoadFramePlugin)
  commandPluginRegistry.registerCommand(new RenameFramePlugin)
  commandPluginRegistry.registerCommand(new RenameColumnsPlugin)
  commandPluginRegistry.registerCommand(new AssignSamplePlugin)
  commandPluginRegistry.registerCommand(new GroupByPlugin)
  commandPluginRegistry.registerCommand(new FlattenColumnPlugin())
  commandPluginRegistry.registerCommand(new BinColumnPlugin)
  commandPluginRegistry.registerCommand(new ColumnModePlugin)
  commandPluginRegistry.registerCommand(new ColumnMedianPlugin)
  commandPluginRegistry.registerCommand(new ColumnSummaryStatisticsPlugin)
  commandPluginRegistry.registerCommand(new CopyPlugin)
  commandPluginRegistry.registerCommand(new CountWherePlugin)
  commandPluginRegistry.registerCommand(new FilterPlugin)
  commandPluginRegistry.registerCommand(new JoinPlugin(frames))
  commandPluginRegistry.registerCommand(new DropColumnsPlugin)
  commandPluginRegistry.registerCommand(new AddColumnsPlugin)
  commandPluginRegistry.registerCommand(new DropDuplicatesPlugin)
  commandPluginRegistry.registerCommand(new QuantilesPlugin)
  commandPluginRegistry.registerCommand(new CovarianceMatrixPlugin)
  commandPluginRegistry.registerCommand(new CovariancePlugin)
  commandPluginRegistry.registerCommand(new ClassificationMetricsPlugin)
  commandPluginRegistry.registerCommand(new EcdfPlugin)
  commandPluginRegistry.registerCommand(new TallyPercentPlugin)
  commandPluginRegistry.registerCommand(new TallyPlugin)
  commandPluginRegistry.registerCommand(new CumulativePercentPlugin)
  commandPluginRegistry.registerCommand(new CumulativeSumPlugin)
  commandPluginRegistry.registerCommand(new ShannonEntropyPlugin)
  commandPluginRegistry.registerCommand(new TopKPlugin)
  commandPluginRegistry.registerCommand(new SortByColumnsPlugin)

  // Registering graph plugins
  commandPluginRegistry.registerCommand(new LoadGraphPlugin)
  commandPluginRegistry.registerCommand(new RenameGraphPlugin)
  commandPluginRegistry.registerCommand(new DefineVertexPlugin(graphs))
  commandPluginRegistry.registerCommand(new DefineEdgePlugin(graphs))
  val addVerticesPlugin = new AddVerticesPlugin(frames, graphs)
  commandPluginRegistry.registerCommand(addVerticesPlugin)
  commandPluginRegistry.registerCommand(new AddEdgesPlugin(addVerticesPlugin))
  commandPluginRegistry.registerCommand(new VertexCountPlugin)
  commandPluginRegistry.registerCommand(new EdgeCountPlugin)
  commandPluginRegistry.registerCommand(new GraphInfoPlugin)
  commandPluginRegistry.registerCommand(new FilterVerticesPlugin(graphs))
  commandPluginRegistry.registerCommand(new DropDuplicateVerticesPlugin(graphs))
  commandPluginRegistry.registerCommand(new RenameVertexColumnsPlugin)
  commandPluginRegistry.registerCommand(new RenameEdgeColumnsPlugin)
  commandPluginRegistry.registerCommand(new DropVertexColumnPlugin)
  commandPluginRegistry.registerCommand(new DropEdgeColumnPlugin)
  commandPluginRegistry.registerCommand(new ExportToTitanGraphPlugin(frames, graphs))
  commandPluginRegistry.registerCommand(new ExportFromTitanPlugin(frames, graphs))

  //Registering model plugins
  commandPluginRegistry.registerCommand(new LogisticRegressionWithSGDTrainPlugin)
  commandPluginRegistry.registerCommand(new LogisticRegressionWithSGDTestPlugin)

  /* This progress listener saves progress update to command table */
  SparkProgressListener.progressUpdater = new SparkCommandStorageUpdater(commandStorage)

  override def getCommands(offset: Int, count: Int)(implicit invocation: Invocation): Future[Seq[Command]] = {
    withContext("se.getCommands") {
      require(offset >= 0, "offset cannot be negative")
      require(count >= 0, "count cannot be negative")
      future {
        commandStorage.scan(offset, Math.min(count, SparkEngineConfig.pageSize))
      }
    }
  }

  override def getCommand(id: Long)(implicit invocation: Invocation): Future[Option[Command]] = withContext("se.getCommand") {
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
  override def getQueries(offset: Int, count: Int)(implicit invocation: Invocation): Future[Seq[Query]] = withContext("se.getQueries") {
    future {
      queryStorage.scan(offset, count)
    }
  }

  /**
   * return a query object
   * @param id query id
   * @return Query
   */
  override def getQuery(id: Long)(implicit invocation: Invocation): Future[Option[Query]] = withContext("se.getQuery") {
    future {
      queryStorage.lookup(id)
    }
  }

  /**
   * returns the data found in a specific query result page
   *
   * @param id query id
   * @param pageId page id
   * @return data of specific page
   */
  override def getQueryPage(id: Long, pageId: Long)(implicit invocation: Invocation) = withContext("se.getQueryPage") {
    withMyClassLoader {
      val ctx = sparkContextFactory.context("query")
      try {
        val data = queryStorage.getQueryPage(ctx, id, pageId)
        com.intel.intelanalytics.domain.query.QueryDataResult(data, None)
      }
      finally {
        ctx.stop()
      }
    }
  }

  override def getUserPrincipal(apiKey: String)(implicit invocation: Invocation): UserPrincipal = {
    users.getUserPrincipal(apiKey)
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @return an Execution that can be used to track the completion of the command
   */
  def execute(command: CommandTemplate)(implicit invocation: Invocation): Execution = {
    commands.execute(command, commandPluginRegistry)
  }

  /**
   * All the command definitions available
   */
  override def getCommandDefinitions()(implicit invocation: Invocation): Iterable[CommandDefinition] =
    withContext("se.getCommandDefinitions") {
      commandPluginRegistry.getCommandDefinitions()
    }

  def create(frame: DataFrameTemplate)(implicit invocation: Invocation): Future[DataFrame] =
    future {
      frames.create(frame)
    }

  def delete(frame: DataFrame)(implicit invocation: Invocation): Future[Unit] = withContext("se.delete") {
    future {
      frames.drop(frame)
    }
  }

  def getFrames()(implicit invocation: Invocation): Future[Seq[DataFrame]] = withContext("se.getFrames") {
    future {
      frames.getFrames()
    }
  }

  def getFrameByName(name: String)(implicit invocation: Invocation): Future[Option[DataFrame]] = withContext("se.getFrameByName") {
    future {
      frames.lookupByName(name)
    }
  }

  /**
   * Execute getRows Query plugin
   * @param arguments RowQuery object describing id, offset, and count
   * @return the QueryExecution
   */
  def getRowsLarge(arguments: RowQuery[Identifier])(implicit invocation: Invocation): PagedQueryResult = {
    val queryExecution = queries.execute(getRowsQuery, arguments)
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
    implicit val inv = invocation
    if (arguments.count + arguments.offset <= SparkEngineConfig.pageSize) {
      val rdd = frames.loadLegacyFrameRdd(invocation.sparkContext, arguments.id).rows
      val takenRows = rdd.take(arguments.count + arguments.offset.toInt).drop(arguments.offset.toInt)
      invocation.sparkContext.parallelize(takenRows)
    }
    else {
      val frame = frames.lookup(arguments.id).getOrElse(throw new IllegalArgumentException("Requested frame does not exist"))
      val rows = frames.getPagedRowsRDD(frame, arguments.offset, arguments.count, invocation.sparkContext)
      rows
    }
  }

  /**
   * Return a sequence of Rows from an RDD starting from a supplied offset
   *
   * @param arguments RowQuery object describing id, offset, and count
   * @return A QueryResult describing the data and schema of this take
   */
  def getRows(arguments: RowQuery[Identifier])(implicit invocation: Invocation): QueryResult = {
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

  def getFrame(id: Identifier)(implicit invocation: Invocation): Future[Option[DataFrame]] =
    withContext("se.getFrame") {
      future {
        frames.lookup(id)
      }
    }

  /**
   * Register a graph name with the metadata store.
   * @param graph Metadata for graph creation.
   * @return Future of the graph to be created.
   */
  def createGraph(graph: GraphTemplate)(implicit invocation: Invocation) = {
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
  def getGraph(id: Identifier)(implicit invocation: Invocation): Future[Graph] = {
    future {
      graphs.lookup(id).get
    }
  }

  /**
   * Get the metadata for a range of graph identifiers.
   * @return Future of the sequence of graph metadata entries to be returned.
   */
  def getGraphs()(implicit invocation: Invocation): Future[Seq[Graph]] =
    withContext("se.getGraphs") {
      future {
        graphs.getGraphs()
      }
    }

  def getGraphByName(name: String)(implicit invocation: Invocation): Future[Option[Graph]] =
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
  def deleteGraph(graph: Graph)(implicit invocation: Invocation): Future[Unit] = {
    withContext("se.deletegraph") {
      future {
        graphs.drop(graph)
      }
    }
  }

  commandPluginRegistry.registerCommand(new DropDuplicatesPlugin)

  /**
   * Register a model name with the metadate store.
   * @param model Metadata for model creation.
   * @return Future of the model to be created.
   */
  def createModel(model: ModelTemplate)(implicit invocation: Invocation) = {
    future {
      withMyClassLoader {
        models.createModel(model)
      }
    }
  }
  /**
   * Obtain a model's metadata from its identifier.
   * @param id Unique identifier for the model provided by the metastore.
   * @return A future of the model metadata entry.
   */
  def getModel(id: Identifier)(implicit invocation: Invocation): Future[Model] = {
    future {
      models.lookup(id).get
    }
  }

  def getModelByName(name: String)(implicit invocation: Invocation): Future[Option[Model]] =
    withContext("se.getModelByName") {
      future {
        models.getModelByName(name)
      }
    }

  def getModels()(implicit invocation: Invocation): Future[Seq[Model]] =
    withContext("se.getModels") {
      future {
        models.getModels()
      }
    }

  /**
   * Delete a model from the metastore.
   * @param model Model
   */
  def deleteModel(model: Model)(implicit invocation: Invocation): Future[Unit] = {
    withContext("se.deletemodel") {
      future {
        models.drop(model)
      }
    }
  }

  override def cancelCommand(id: Long)(implicit invocation: Invocation): Future[Unit] = withContext("se.cancelCommand") {
    future {
      commands.stopCommand(id)
    }
  }

  override def shutdown(): Unit = {
    //do nothing
  }

  override def getVertex(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[Option[DataFrame]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      Some(seamless.vertexMeta(label))
    }
  }

  override def getVertices(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[DataFrame]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.vertexFrames
    }
  }

  override def getEdge(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[Option[DataFrame]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      Some(seamless.edgeMeta(label))
    }
  }

  override def getEdges(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[DataFrame]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.edgeFrames
    }
  }
}
