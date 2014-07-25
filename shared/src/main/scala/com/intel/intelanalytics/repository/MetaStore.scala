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

package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.{ User, UserTemplate, Status }
import com.intel.intelanalytics.domain.frame.{ DataFrame, DataFrameTemplate }
import com.intel.intelanalytics.domain.graph.{ Graph, GraphTemplate }
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import com.intel.intelanalytics.repository._
import com.intel.intelanalytics.domain.Status
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.Status
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.Status
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.Status
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.Status
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.UserTemplate
import com.intel.intelanalytics.domain.graph.GraphTemplate

/**
 * The MetaStore gives access to Repositories. Repositories are how you
 * modify and query underlying tables (frames, graphs, users, etc).
 */
trait MetaStore {
  type Session
  def withSession[T](name: String)(f: Session => T): T

  /** Repository for CRUD on 'status' table */
  def statusRepo: Repository[Session, Status, Status]

  /** Repository for CRUD on 'frame' table */
  //def frameRepo: Repository[Session, DataFrameTemplate, DataFrame]
  def frameRepo: FrameRepository[Session]

  /** Repository for CRUD on 'graph' table */
  def graphRepo: Repository[Session, GraphTemplate, Graph]

  /** Repository for CRUD on 'command' table */
  def commandRepo: CommandRepository[Session]

  /** Repository for CRUD on 'query' table */
  def queryRepo: QueryRepository[Session]

  /** Repository for CRUD on 'user' table */
  def userRepo: Repository[Session, UserTemplate, User] with Queryable[Session, User]

  /** Create the underlying tables */
  def initializeSchema(): Unit

  /** Delete ALL of the underlying tables - useful for unit tests only */
  private[repository] def dropAllTables(): Unit
}
