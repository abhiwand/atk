package com.intel.intelanalytics.repository

import scala.util.Try
import com.intel.intelanalytics.domain.HasId


trait ReadRepository[Session, Entity <: HasId] {
  def defaultScanCount: Int = 20 //TODO: move to config
  def lookup(id: Long) (implicit session: Session) : Option[Entity]
  def scan(offset: Int = 0, count: Int = defaultScanCount) (implicit session: Session): Seq[Entity]
}

trait Repository[Session, CreateEntity, Entity <: HasId] extends ReadRepository[Session, Entity] {
  def insert(entity: CreateEntity) (implicit session: Session): Try[Entity]
  def update(entity: Entity) (implicit session: Session): Try[Entity]
  def delete(id: Long) (implicit session: Session): Try[Unit]
}

