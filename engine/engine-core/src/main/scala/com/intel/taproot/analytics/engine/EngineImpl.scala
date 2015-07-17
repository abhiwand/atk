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

package com.intel.taproot.analytics.engine

import java.util.{ArrayList => JArrayList, List => JList}

import com.intel.taproot.analytics.component.ClassLoaderAware
import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.command.{Command, CommandDefinition, CommandTemplate, Execution}
import com.intel.taproot.analytics.domain.frame.{FrameEntity, FrameReference}
import com.intel.taproot.analytics.domain.graph._
import com.intel.taproot.analytics.domain.model.{ModelEntity, ModelReference}
import com.intel.taproot.analytics.domain.query._
import com.intel.taproot.analytics.engine.plugin.Invocation
import com.intel.taproot.analytics.engine.spark.command.{CommandExecutor, CommandPluginRegistry}
import com.intel.taproot.analytics.engine.spark.frame._
import com.intel.taproot.analytics.engine.spark.gc.{GarbageCollectionPlugin, GarbageCollector}
import com.intel.taproot.analytics.engine.spark.graph.SparkGraphStorage
import com.intel.taproot.analytics.engine.spark.model.SparkModelStorage
import com.intel.taproot.analytics.engine.spark.partitioners.SparkAutoPartitioner
import com.intel.taproot.analytics.engine.spark.threading.EngineExecutionContext.global
import com.intel.taproot.analytics.engine.spark.user.UserStorage
import com.intel.taproot.analytics.security.UserPrincipal
import com.intel.taproot.analytics.{EventLoggingImplicits, NotFoundException}
import com.intel.taproot.event.EventLogging
import org.apache.spark.SparkContext
import org.apache.spark.engine.SparkProgressListener
import org.apache.spark.frame.FrameRdd

import scala.concurrent._
import scala.util.{Failure, Success, Try}

object EngineImpl {
  private val pythonRddDelimiter = "YoMeDelimiter"
}

class EngineImpl(val sparkContextFactory: SparkContextFactory,
                 commands: CommandExecutor,
                 commandStorage: CommandStorage,
                 val frames: SparkFrameStorage,
                 val graphs: SparkGraphStorage,
                 val models: SparkModelStorage,
                 users: UserStorage,
                 val sparkAutoPartitioner: SparkAutoPartitioner,
                 commandPluginRegistry: CommandPluginRegistry) extends Engine
    with EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  type Data = FrameRdd
  type Context = SparkContext

  val fsRoot = EngineConfig.fsRoot
  override val pageSize: Int = EngineConfig.pageSize

  // Administrative plugins
  commandPluginRegistry.registerCommand(new GarbageCollectionPlugin)

  /* This progress listener saves progress update to command table */
  SparkProgressListener.progressUpdater = new CommandStorageProgressUpdater(commandStorage)

  override def getCommands(offset: Int, count: Int)(implicit invocation: Invocation): Future[Seq[Command]] = {
    withContext("se.getCommands") {
      require(offset >= 0, "offset cannot be negative")
      require(count >= 0, "count cannot be negative")
      future {
        commandStorage.scan(offset, Math.min(count, EngineConfig.pageSize))
      }
    }
  }

  override def getCommand(id: Long)(implicit invocation: Invocation): Future[Option[Command]] = withContext("se.getCommand") {
    future {
      commandStorage.lookup(id)
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

  //  /**
  //   * Score a vector on a model.
  //   * @param name Model name
  //   */
  //  override def scoreModel(name: String, values: String): Future[Double] = future {
  //    val vector = DataTypes.toVector(-1)(values)
  //    val model = models.getModelByName(Some(name))
  //    model match {
  //      case Some(x) => {
  //            x.modelType match {
  //              case "model:libsvm" => {
  //                val svmJsObject = x.data.getOrElse(throw new RuntimeException("Can't score because model has not been trained yet"))
  //                val libsvmData = svmJsObject.convertTo[LibSvmData]
  //                val libsvmModel = libsvmData.svmModel
  //                val predictionLabel = LibSvmPluginFunctions.score(libsvmModel, vector)
  //                predictionLabel.value
  //              }
  //          case _ => throw new IllegalArgumentException("Only libsvm Model is supported for scoring at this time")
  //        }
  //      }
  //      case None => throw new IllegalArgumentException(s"Model with the provided name '$name' does not exist in the metastore")
  //    }
  //  }

  override def cancelCommand(id: Long)(implicit invocation: Invocation): Future[Unit] = withContext("se.cancelCommand") {
    future {
      commands.cancelCommand(id, commandPluginRegistry)
    }
  }

  override def shutdown(): Unit = {
    GarbageCollector.shutdown()
  }

  override def getVertex(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[FrameEntity] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.vertexMeta(label)
    }
  }

  override def getVertices(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.vertexFrames
    }
  }

  override def getEdge(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[FrameEntity] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.edgeMeta(label)
    }
  }

  override def getEdges(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]] = {
    future {
      val seamless = graphs.expectSeamless(graphId)
      seamless.edgeFrames
    }
  }
}
