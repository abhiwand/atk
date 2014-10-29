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

package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.{ StorageFormats, IAUri, HasId }
import org.joda.time.DateTime

/**
 * The metadata entry for a graph.
 * @param id unique id auto-generated by the database
 * @param name name assigned by user, auto-assigned by system if user doesn't supply
 * @param description description of the graph (a good default might say what frames it came from)
 * @param storage name used in physical data store, the HBase table name
 * @param statusId lifecycle status. For example, INIT (building), ACTIVE, DELETED (un-delete possible), DELETE_FINAL (no un-delete), INCOMPLETE (failed construction)
 * @param storageFormat e.g. "ia/frame", "cassandra/titan", "hbase/titan"
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 * @param createdByUserId user who created this row
 * @param modifiedByUserId  user who last modified this row
 * @param idCounter idCounter counter for generating unique ids for vertices and edges with Seamless Graph.
 */
case class Graph(id: Long,
                 name: String,
                 description: Option[String],
                 storage: String,
                 statusId: Long,
                 storageFormat: String,
                 createdOn: DateTime,
                 modifiedOn: DateTime,
                 createdByUserId: Option[Long] = None,
                 modifiedByUserId: Option[Long] = None,
                 idCounter: Option[Long] = None) extends HasId with IAUri {
  require(id >= 0, "id must be zero or greater")
  require(name != null, "name must not be null")
  require(name.trim.length > 0, "name must not be empty or whitespace")
  def entity = "graph"
  StorageFormats.validateGraphFormat(storageFormat)

  def isSeamless: Boolean = {
    StorageFormats.isSeamlessGraph(storageFormat)
  }

  def isTitan: Boolean = {
    !StorageFormats.isSeamlessGraph(storageFormat)
  }

  def commandPrefix: String = {
    if (isTitan) "graph:titan"
    else if (isSeamless) "graph:"
    else throw new RuntimeException("New graph type is not yet implemented!")
  }

  /**
   * Get the next id from the idCounter
   */
  def nextId(): Long = {
    idCounter.getOrElse(0L) + 1L
  }
}
