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

package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphTemplate }

/**
 * Repository for graphs
 */
trait GraphRepository[Session] extends Repository[Session, GraphTemplate, GraphEntity] with NameableRepository[Session, GraphEntity] with GarbageCollectableRepository[Session, GraphEntity] {
  /**
   * Return all the graphs
   * @param session current session
   * @return all the graphs
   */
  def scanAll()(implicit session: Session): Seq[GraphEntity]

  def updateIdCounter(id: Long, idCounter: Long)(implicit session: Session): Unit

  /**
   * Returns the liveness of the graph. If it is named or has named frames then it is a live graph.
   * @param id id of graph in question
   * @return true if graph is live, false if it is not
   */
  def isLive(id: GraphEntity)(implicit session: Session): Boolean

}