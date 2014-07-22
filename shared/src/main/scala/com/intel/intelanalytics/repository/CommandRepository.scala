package com.intel.intelanalytics.repository
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import scala.util.Try
import com.intel.intelanalytics.engine.ProgressInfo

/**
 * Repository for command records
 */
trait CommandRepository[Session] extends Repository[Session, CommandTemplate, Command] {
  def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit]
  def updateProgress(id: Long, progressInfo: List[ProgressInfo])(implicit session: Session): Try[Unit]
}

