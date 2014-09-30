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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.command.{ CommandDefinition, Execution, CommandTemplate, Command }
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.frame.load.Load
import com.intel.intelanalytics.domain.graph.{ Graph, GraphLoad, GraphTemplate, RenameGraph }
import com.intel.intelanalytics.domain.query.{ Execution => QueryExecution, PagedQueryResult, QueryDataResult, RowQuery, Query }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

import scala.concurrent.Future
import com.intel.intelanalytics.domain.schema.Schema

//TODO: make these all use Try instead?
//TODO: make as many of these as possible use id instead of dataframe as the first argument?
//TODO: distinguish between DataFrame and DataFrameSpec,
// where the latter has no ID, and is the argument passed to create?
trait Engine {

  type Identifier = Long //TODO: make more generic?
  val pageSize: Int

  val frames: FrameStorage

  val graphs: GraphStorage

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @param user the user running the command
   * @return an Execution that can be used to track the completion of the command
   */
  def execute(command: CommandTemplate)(implicit user: UserPrincipal): Execution

  /**
   * All the command definitions available
   */
  def getCommandDefinitions()(implicit user: UserPrincipal): Iterable[CommandDefinition]

  def getCommands(offset: Int, count: Int): Future[Seq[Command]]

  def getCommand(id: Identifier): Future[Option[Command]]

  def getQueries(offset: Int, count: Int): Future[Seq[Query]]

  def getQuery(id: Identifier): Future[Option[Query]]

  def getQueryPage(id: Identifier, pageId: Identifier)(implicit user: UserPrincipal): QueryDataResult

  def getUserPrincipal(apiKey: String): UserPrincipal

  def getFrame(id: Identifier)(implicit user: UserPrincipal): Future[Option[DataFrame]]

  def getRows(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): Future[QueryDataResult]

  def getRowsLarge(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): PagedQueryResult

  def create(frame: DataFrameTemplate)(implicit user: UserPrincipal): Future[DataFrame]

  def delete(frame: DataFrame): Future[Unit]

  def getFrames()(implicit p: UserPrincipal): Future[Seq[DataFrame]]

  def getFrameByName(name: String)(implicit p: UserPrincipal): Future[Option[DataFrame]]

  def shutdown(): Unit

  def getGraph(id: Identifier): Future[Graph]

  def getGraphs()(implicit user: UserPrincipal): Future[Seq[Graph]]

  def getGraphByName(name: String)(implicit user: UserPrincipal): Future[Option[Graph]]

  def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Future[Graph]

  def deleteGraph(graph: Graph): Future[Unit]

  /**
   * Cancel a running command
   * @param id command id
   * @param user current user
   * @return optional command instance
   */
  def cancelCommand(id: Identifier)(implicit user: UserPrincipal): Future[Unit]
}
