/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark

import java.util.{ ArrayList => JArrayList, List => JList }

import com.intel.event.{ EventLogging }
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.model.{ ModelReference, ModelEntity, ModelTemplate }
import com.intel.intelanalytics.engine.spark.gc.{ GarbageCollectionPlugin, GarbageCollector }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, CommandPluginRegistry }

import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.partitioners.SparkAutoPartitioner
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.libSvmPlugins._
import com.intel.intelanalytics.{ EventLoggingImplicits, NotFoundException }
import org.apache.spark.SparkContext
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.queries.SparkQueryStorage
import com.intel.intelanalytics.engine.{ ProgressInfo, _ }
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.libsvm.ia.plugins.LibSvmJsonProtocol._
import spray.json._
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import org.apache.spark.frame.FrameRdd

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import org.apache.spark.engine.SparkProgressListener
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.domain.{ CreateEntityArgs }

import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.query._
import com.intel.intelanalytics.domain.command.CommandDefinition
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.command.Execution
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.engine.spark.user.UserStorage
import scala.util.{ Try, Success, Failure }

object SparkEngine {
  private val pythonRddDelimiter = "YoMeDelimiter"
}

class SparkEngine(val sparkContextFactory: SparkContextFactory,
                  commands: CommandExecutor,
                  commandStorage: CommandStorage,
                  val frames: SparkFrameStorage,
                  val graphs: SparkGraphStorage,
                  val models: SparkModelStorage,
                  users: UserStorage,
                  queryStorage: SparkQueryStorage,
                  val sparkAutoPartitioner: SparkAutoPartitioner,
                  commandPluginRegistry: CommandPluginRegistry) extends Engine
    with EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  type Data = FrameRdd
  type Context = SparkContext

  val fsRoot = SparkEngineConfig.fsRoot
  override val pageSize: Int = SparkEngineConfig.pageSize

  // TODO: all plugins should move out of engine-core into plugin modules
  commandPluginRegistry.registerCommand(new LibSvmPredictPlugin)
  commandPluginRegistry.registerCommand(new LibSvmPlugin)
  commandPluginRegistry.registerCommand(new LibSvmTrainPlugin)
  commandPluginRegistry.registerCommand(new LibSvmScorePlugin)
  commandPluginRegistry.registerCommand(new LibSvmTestPlugin)
  commandPluginRegistry.registerCommand(new LibSvmPredictPlugin)

  // Administrative plugins
  commandPluginRegistry.registerCommand(new GarbageCollectionPlugin)

  /* This progress listener saves progress update to command table */
  SparkProgressListener.progressUpdater = new CommandStorageProgressUpdater(commandStorage)

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
      val sc = sparkContextFactory.context("query")
      try {
        val data = queryStorage.getQueryPage(sc, id, pageId)
        com.intel.intelanalytics.domain.query.QueryDataResult(data, None)
      }
      finally {
        if (SparkEngineConfig.reuseSparkContext) {
          info("not stopping local SparkContext so that it can be re-used")
        }
        else {
          sc.stop()
        }
      }
    }
  }

  override def getUserPrincipal(apiKey: String)(implicit invocation: Invocation): UserPrincipal = {
    users.getUserPrincipal(apiKey)
  }

  override def addUserPrincipal(userName: String)(implicit invocation: Invocation): UserPrincipal = {
    Try { users.getUserPrincipal(userName) } match {
      case Success(found) => throw new RuntimeException(s"User $userName already exists, cannot add it.")
      case Failure(missing) =>
        println(s"Adding new user $userName")
        users.insertUser(userName)
    }
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
      commandPluginRegistry.commandDefinitions
    }

  @deprecated("use engine.graphs.createFrame()")
  def createFrame(arguments: CreateEntityArgs)(implicit invocation: Invocation): Future[FrameEntity] =
    future {
      frames.create(arguments)
    }

  override def deleteFrame(id: Identifier)(implicit invocation: Invocation): Future[Unit] = withContext("se.delete") {
    future {
      val frame = frames.expectFrame(FrameReference(id))
      frames.scheduleDeletion(frame)
    }
  }

  def getFrames()(implicit invocation: Invocation): Future[Seq[FrameEntity]] = withContext("se.getFrames") {
    future {
      frames.getFrames()
    }
  }

  def getFrameByName(name: String)(implicit invocation: Invocation): Future[Option[FrameEntity]] = withContext("se.getFrameByName") {
    future {
      val frame = frames.lookupByName(Some(name))
      frame
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
      val rows = frames.getRows(frame, arguments.offset, arguments.count)
      QueryDataResult(rows, Some(frame.schema))
    }
  }

  def getFrame(id: Identifier)(implicit invocation: Invocation): Future[Option[FrameEntity]] =
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
  @deprecated("use engine.graphs.createGraph()")
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
  def getGraph(id: Identifier)(implicit invocation: Invocation): Future[GraphEntity] = {
    future {
      graphs.lookup(id).getOrElse(throw new NotFoundException("graph"))
    }
  }

  /**
   * Get the metadata for a range of graph identifiers.
   * @return Future of the sequence of graph metadata entries to be returned.
   */
  def getGraphs()(implicit invocation: Invocation): Future[Seq[GraphEntity]] =
    withContext("se.getGraphs") {
      future {
        graphs.getGraphs()
      }
    }

  def getGraphByName(name: String)(implicit invocation: Invocation): Future[Option[GraphEntity]] =
    withContext("se.getGraphByName") {
      future {
        val graph = graphs.getGraphByName(Some(name))
        graph
      }
    }

  /**
   * Schedule Delete a graph from the graph database.
   * @param graphId The graph to be deleted.
   * @return A future of unit.
   */
  override def deleteGraph(graphId: Identifier)(implicit invocation: Invocation): Future[Unit] = {
    withContext("se.deletegraph") {
      future {
        val graph = graphs.expectGraph(GraphReference(graphId))
        graphs.scheduleDeletion(graph)
      }
    }
  }

  /**
   * Register a model name with the metadate store.
   * @param createArgs create entity args
   * @return Future of the model to be created.
   */
  def createModel(createArgs: CreateEntityArgs)(implicit invocation: Invocation) = {
    future {
      withMyClassLoader {
        models.createModel(createArgs)
      }
    }
  }
  /**
   * Obtain a model's metadata from its identifier.
   * @param id Unique identifier for the model provided by the metastore.
   * @return A future of the model metadata entry.
   */
  def getModel(id: Identifier)(implicit invocation: Invocation): Future[ModelEntity] = {
    future {
      models.expectModel(ModelReference(id))
    }
  }

  def getModelByName(name: String)(implicit invocation: Invocation): Future[Option[ModelEntity]] =
    withContext("se.getModelByName") {
      future {
        val model = models.getModelByName(Some(name))
        model
      }
    }

  def getModels()(implicit invocation: Invocation): Future[Seq[ModelEntity]] =
    withContext("se.getModels") {
      future {
        models.getModels()
      }
    }

  /**
   * Delete a model from the metastore.
   * @param id Model id
   */
  override def deleteModel(id: Identifier)(implicit invocation: Invocation): Future[Unit] = {
    withContext("se.deletemodel") {
      future {
        val model = models.expectModel(ModelReference(id))
        models.scheduleDeletion(model)
      }
    }
  }

  /**
   * Score a vector on a model.
   * @param name Model name
   */
  override def scoreModel(name: String, values: String): Future[Double] = future {
    val vector = DataTypes.toVector(-1)(values)
    val model = models.getModelByName(Some(name))
    model match {
      case Some(x) => {
        x.modelType match {
          case "model:libsvm" => {
            val svmJsObject = x.data.getOrElse(throw new RuntimeException("Can't score because model has not been trained yet"))
            val libsvmData = svmJsObject.convertTo[LibSvmData]
            val libsvmModel = libsvmData.svmModel
            val predictionLabel = LibSvmPluginFunctions.score(libsvmModel, vector)
            predictionLabel.value
          }
          case _ => throw new IllegalArgumentException("Only libsvm Model is supported for scoring at this time")
        }
      }
      case None => throw new IllegalArgumentException(s"Model with the provided name '$name' does not exist in the metastore")
    }
  }

  override def cancelCommand(id: Long)(implicit invocation: Invocation): Future[Unit] = withContext("se.cancelCommand") {
    future {
      commands.cancelCommand(id, commandPluginRegistry)
    }
  }

  override def shutdown(): Unit = {
    GarbageCollector.shutdown()
  }

  override def getVertex(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[Option[FrameEntity]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      Some(seamless.vertexMeta(label))
    }
  }

  override def getVertices(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.vertexFrames
    }
  }

  override def getEdge(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[Option[FrameEntity]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      Some(seamless.edgeMeta(label))
    }
  }

  override def getEdges(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.edgeFrames
    }
  }
}
