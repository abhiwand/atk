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

package com.intel.intelanalytics.domain

import org.joda.time.DateTime

/**
 * Lifecycle Status for Graphs and Frames
 * @param id unique id in the database
 * @param name the short name, For example, INIT (building), ACTIVE, DELETED (undelete possible), DELETE_FINAL (no undelete), INCOMPLETE (failed construction)
 * @param description two or three sentence description of the status
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class Status(id: Long, name: String, description: String, createdOn: DateTime, modifiedOn: DateTime) extends HasId {

  require(name != null)

  /** Initial Status, currently building or initializing */
  def isInit: Boolean = id.equals(1)

  /** Active and can be interacted with */
  def isActive: Boolean = id.equals(2)

  /** Partially created, failure occurred during construction */
  def isIncomplete: Boolean = id.equals(3)

  /** Deleted but can still be un-deleted, no action has yet been taken on disk */
  def isDeleted: Boolean = id.equals(4)

  /** Underlying storage has been reclaimed, no un-delete is possible */
  def isDeleteFinal: Boolean = id.equals(5)
}
