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
