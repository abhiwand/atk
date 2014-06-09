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

import scala.concurrent.Future
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.frame._
import spray.json.JsObject
import com.intel.intelanalytics.domain.frame.FrameRenameColumn
import com.intel.intelanalytics.domain.frame.FrameProject
import com.intel.intelanalytics.domain.frame.FrameRenameFrame
import com.intel.intelanalytics.domain.FilterPredicate
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.frame.FrameRemoveColumn
import com.intel.intelanalytics.domain.frame.FrameJoin
import com.intel.intelanalytics.domain.frame.LoadLines
import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.domain.frame.FrameAddColumn
import com.intel.intelanalytics.domain.graph.{GraphLoad, GraphTemplate, Graph}

//TODO: make these all use Try instead?
//TODO: make as many of these as possible use id instead of dataframe as the first argument?
//TODO: distinguish between DataFrame and DataFrameSpec,
// where the latter has no ID, and is the argument passed to create?
trait Engine {

  type Identifier = Long //TODO: make more generic?


  //TODO: We'll probably return an Iterable[Vertex] instead of rows at some point.
  def getVertices(graph: Identifier, offset: Int, count: Int, queryName: String, parameters: Map[String, String]): Future[Iterable[Row]]

  def getCommands(offset: Int, count: Int): Future[Seq[Command]]

  def getCommand(id: Identifier): Future[Option[Command]]

  def getFrame(id: Identifier): Future[Option[DataFrame]]

  def getRows(id: Identifier, offset: Long, count: Int)(implicit user: UserPrincipal): Future[Iterable[Row]]

  def create(frame: DataFrameTemplate): Future[DataFrame]

  def clear(frame: DataFrame): Future[DataFrame]

  def load(arguments: LoadLines[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def filter(arguments: FilterPredicate[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def project(arguments: FrameProject[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def renameFrame(arguments: FrameRenameFrame[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def renameColumn(arguments: FrameRenameColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  //  Should predicate be Partial[Any]  def filter(frame: DataFrame, predicate: Partial[Any])(implicit user: UserPrincipal): Future[DataFrame]
  def removeColumn(arguments: FrameRemoveColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def addColumn(arguments: FrameAddColumn[JsObject, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def alter(frame: DataFrame, changes: Seq[Alteration])

  def delete(frame: DataFrame): Future[Unit]
  def join(argument: FrameJoin)(implicit user: UserPrincipal): (Command, Future[Command])
  def flattenColumn(argument: FlattenColumn[Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def getFrames(offset: Int, count: Int)(implicit p: UserPrincipal): Future[Seq[DataFrame]]

  def shutdown: Unit

  def getGraph(id: Identifier): Future[Graph]

  def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Future[Seq[Graph]]

  def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Future[Graph]

  def loadGraph(graph: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): (Command, Future[Command])

  def deleteGraph(graph: Graph): Future[Unit]

}
