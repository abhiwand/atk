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

import com.intel.intelanalytics.domain.command.{ Execution, CommandTemplate }
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.frame.load.Load
import com.intel.intelanalytics.domain.graph.{ Graph, GraphLoad, GraphTemplate }
import com.intel.intelanalytics.domain.query.RowQuery
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

import scala.concurrent.Future

//TODO: make these all use Try instead?
//TODO: make as many of these as possible use id instead of dataframe as the first argument?
//TODO: distinguish between DataFrame and DataFrameSpec,
// where the latter has no ID, and is the argument passed to create?
trait Engine {

  type Identifier = Long //TODO: make more generic?

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

  //TODO: We'll probably return an Iterable[Vertex] instead of rows at some point.
  def getVertices(graph: Identifier, offset: Int, count: Int, queryName: String, parameters: Map[String, String]): Future[Iterable[Row]]

  def getCommands(offset: Int, count: Int): Future[Seq[Command]]

  def getCommand(id: Identifier): Future[Option[Command]]

  def getFrame(id: Identifier): Future[Option[DataFrame]]

  def getRows(arguments: RowQuery[Identifier])(implicit user: UserPrincipal): Execution

  def create(frame: DataFrameTemplate)(implicit user: UserPrincipal): Future[DataFrame]

  def load(arguments: Load[Long])(implicit user: UserPrincipal): Execution

  def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): Execution

  def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): Execution

  def renameFrame(arguments: FrameRenameFrame[JsObject, Long])(implicit user: UserPrincipal): Execution

  def renameColumn(arguments: FrameRenameColumn[JsObject, Long])(implicit user: UserPrincipal): Execution

  def removeColumn(arguments: FrameRemoveColumn[JsObject, Long])(implicit user: UserPrincipal): Execution

  def addColumns(arguments: FrameAddColumns[JsObject, Long])(implicit user: UserPrincipal): Execution

  def delete(frame: DataFrame): Future[Unit]

  /**
   * Remove duplicates rows, keeping only one row per uniqueness criteria match
   * @param dropDuplicateCommand command for dropping duplicates
   * @param user current user
   */
  def dropDuplicates(dropDuplicateCommand: DropDuplicates)(implicit user: UserPrincipal): Execution

  def join(argument: FrameJoin)(implicit user: UserPrincipal): Execution
  def flattenColumn(argument: FlattenColumn)(implicit user: UserPrincipal): Execution

  def binColumn(arguments: BinColumn[Long])(implicit user: UserPrincipal): Execution

  def groupBy(arguments: FrameGroupByColumn[JsObject, Long])(implicit user: UserPrincipal): Execution

  def getFrames(offset: Int, count: Int)(implicit p: UserPrincipal): Future[Seq[DataFrame]]

  def shutdown(): Unit

  def getGraph(id: Identifier): Future[Graph]

  def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Future[Seq[Graph]]

  def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Future[Graph]

  def loadGraph(graph: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): Execution

  def deleteGraph(graph: Graph): Future[Unit]

}
