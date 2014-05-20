package com.intel.intelanalytics.repository

import scala.util.Try
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

trait Queryable[Session, Entity <: HasId] {

  /**
   * Retrieve all rows that have a particular column value
   *
   * @param colName column to query on
   * @param value value to look for
   * @return zero or more matching rows
   */
  def retrieveByColumnValue(colName: String, value: String)(implicit session: Session): List[Entity]
}

