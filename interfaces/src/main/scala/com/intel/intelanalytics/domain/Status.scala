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

  require(name != null, "name must not be null")

  /** Active and can be interacted with */
  def isActive: Boolean = id.equals(Status.Active)

  /** Deleted but can still be un-deleted, no action has yet been taken on disk */
  def isDeleted: Boolean = id.equals(Status.Deleted)

  /** Underlying storage has been reclaimed, no un-delete is possible */
  def isDeleteFinal: Boolean = id.equals(Status.Deleted_Final)
}

object Status {

  /**
   * Return the proper id for a read garbage collectible entity
   * @param id the status id before the read
   * @return proper status id for after the read
   */
  def getNewStatusForRead(id: Long): Long =
    if (id == Deleted)
      Active
    else
      id

  def getName(id: Long): String = {
    id match {
      case Active => "Active"
      case Deleted => "Deleted (scheduled may be undeleted by modifying or inspecting)"
      case Deleted_Final => "Deleted Final"
      case _ => "Unkown"
    }
  }

  /** Active and can be interacted with */
  final val Active: Long = 1

  /** User has marked as Deleted but can still be un-deleted, no action has yet been taken on disk */
  final val Deleted: Long = 2

  /** Underlying storage has been reclaimed, no un-delete is possible */
  final val Deleted_Final: Long = 3
}
