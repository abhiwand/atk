package com.intel.intelanalytics.repository

import scala.util.Try
import com.intel.intelanalytics.domain.HasId



/**
 * Repository interface for read/write operations (CRUD) for a single table.
 *
 * @tparam CreateEntity type of entity for initial creation
 * @tparam Entity type of entity
 */
trait Repository[Session, CreateEntity, Entity <: HasId] extends ReadRepository[Session, Entity] {

  /**
   * Insert a row
   * @param entity values to insert
   * @return the newly created Entity
   */
  def insert(entity: CreateEntity)(implicit session: Session): Try[Entity]

  /**
   * Update a row
   * @return the updated Entity
   */
  def update(entity: Entity)(implicit session: Session): Try[Entity]

  /**
   * Row to delete
   */
  def delete(id: Long)(implicit session: Session): Try[Unit]
}



