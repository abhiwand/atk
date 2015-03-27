//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine

import com.intel.event.EventContext
import com.intel.intelanalytics.domain.{ CreateEntityArgs, UriReference, EntityType }
import com.intel.intelanalytics.domain.command.{ Command, CommandDefinition, CommandTemplate, Execution }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphTemplate }
import com.intel.intelanalytics.domain.model.{ ModelEntity, ModelTemplate }
import com.intel.intelanalytics.domain.query.{ PagedQueryResult, Query, QueryDataResult, RowQuery, Execution => QueryExecution, _ }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.Future

trait Engine {

  type Identifier = Long //TODO: make more generic?

  val pageSize: Int

  val frames: FrameStorage

  val graphs: GraphStorage

  val models: ModelStorage

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @return an Execution that can be used to track the completion of the command
   */
  def execute(command: CommandTemplate)(implicit invocation: Invocation): Execution

  /**
   * All the command definitions available
   */
  def getCommandDefinitions()(implicit invocation: Invocation): Iterable[CommandDefinition]

  def getCommands(offset: Int, count: Int)(implicit invocation: Invocation): Future[Seq[Command]]

  def getCommand(id: Identifier)(implicit invocation: Invocation): Future[Option[Command]]

  def getQueries(offset: Int, count: Int)(implicit invocation: Invocation): Future[Seq[Query]]

  def getQuery(id: Identifier)(implicit invocation: Invocation): Future[Option[Query]]

  def getQueryPage(id: Identifier, pageId: Identifier)(implicit invocation: Invocation): QueryDataResult

  //  def getEntityTypes()(implicit invocation: Invocation): Future[Seq[EntityType]]

  def getUserPrincipal(apiKey: String)(implicit invocation: Invocation): UserPrincipal

  def getFrame(id: Identifier)(implicit invocation: Invocation): Future[Option[FrameEntity]]

  def getRows(arguments: RowQuery[Identifier])(implicit invocation: Invocation): QueryResult

  def getRowsLarge(arguments: RowQuery[Identifier])(implicit invocation: Invocation): PagedQueryResult

  @deprecated("use engine.graphs.createFrame()")
  def createFrame(arguments: CreateEntityArgs)(implicit invocation: Invocation): Future[FrameEntity]

  def deleteFrame(id: Identifier)(implicit invocation: Invocation): Future[Unit]

  def getFrames()(implicit invocation: Invocation): Future[Seq[FrameEntity]]

  def getFrameByName(name: String)(implicit invocation: Invocation): Future[Option[FrameEntity]]

  def shutdown(): Unit

  def getGraph(id: Identifier)(implicit invocation: Invocation): Future[GraphEntity]

  def getGraphs()(implicit invocation: Invocation): Future[Seq[GraphEntity]]

  def getGraphByName(name: String)(implicit invocation: Invocation): Future[Option[GraphEntity]]

  @deprecated("use engine.graphs.createGraph()")
  def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): Future[GraphEntity]

  def getVertex(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[Option[FrameEntity]]

  def getVertices(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]]

  def getEdge(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[Option[FrameEntity]]

  def getEdges(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]]

  def deleteGraph(graphId: Identifier)(implicit invocation: Invocation): Future[Unit]

  def createModel(arguments: CreateEntityArgs)(implicit invocation: Invocation): Future[ModelEntity]

  def getModel(id: Identifier)(implicit invocation: Invocation): Future[ModelEntity]

  def getModels()(implicit invocation: Invocation): Future[Seq[ModelEntity]]

  def getModelByName(name: String)(implicit invocation: Invocation): Future[Option[ModelEntity]]

  def deleteModel(id: Identifier)(implicit invocation: Invocation): Future[Unit]

  def scoreModel(id: Identifier, values: Vector[Double])(implicit invocation: Invocation): Future[Double]

  /**
   * Cancel a running command
   * @param id command id
   * @return optional command instance
   */
  def cancelCommand(id: Identifier)(implicit invocation: Invocation): Future[Unit]
}
