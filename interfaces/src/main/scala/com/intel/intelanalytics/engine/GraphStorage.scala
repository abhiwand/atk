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

import com.intel.intelanalytics.domain.graph.{ GraphReference, GraphEntity, LoadGraphArgs, GraphTemplate }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphReference, GraphTemplate }
import com.intel.intelanalytics.security.UserPrincipal

/**
 * Manages multiple graphs in the underlying graph database.
 */
trait GraphStorage {

  /** Lookup a Graph, throw an Exception if not found */
  def expectGraph(graphId: Long)(implicit invocation: Invocation): GraphEntity

  /** Lookup a Graph, throw an Exception if not found */
  def expectGraph(graphRef: GraphReference)(implicit invocation: Invocation): GraphEntity

  @deprecated("please use expectGraph() instead")
  def lookup(id: Long)(implicit invocation: Invocation): Option[GraphEntity]

  def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): GraphEntity

  def renameGraph(graph: GraphEntity, newName: String)(implicit invocation: Invocation): GraphEntity

  def drop(graph: GraphEntity)(implicit invocation: Invocation)

  def copyGraph(graph: GraphEntity, name: Option[String])(implicit invocation: Invocation): GraphEntity

  def updateStatus(graph: GraphEntity, newStatusId: Long)

  def getGraphs()(implicit invocation: Invocation): Seq[GraphEntity]

  def getGraphByName(name: Option[String])(implicit invocation: Invocation): Option[GraphEntity]

}
