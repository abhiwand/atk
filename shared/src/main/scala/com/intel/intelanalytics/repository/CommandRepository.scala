package com.intel.intelanalytics.repository
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import scala.util.Try

/**
 * Repository for command records
 */
trait CommandRepository[Session] extends Repository[Session, CommandTemplate, Command] {
  def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit]
  def updateProgress(id: Long, progress: List[Float])(implicit session: Session): Try[Unit]
}

