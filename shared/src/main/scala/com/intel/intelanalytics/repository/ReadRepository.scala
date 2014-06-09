package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.HasId

/**
 * Repository interface for read-only operations (basic look-ups) for a single table.
 *
 * @tparam Entity type of entity
 */
trait ReadRepository[Session, Entity <: HasId] {

  def defaultScanCount: Int = 20 //TODO: move to config

  /**
   * Find by Primary Key
   */
  def lookup(id: Long)(implicit session: Session): Option[Entity]

  /**
   * Get a 'page' of rows
   *
   * @param offset for pagination, start at '0' and increment
   * @param count number of rows to return
   */
  def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Entity]
}
