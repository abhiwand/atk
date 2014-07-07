package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.query.{ QueryTemplate, Query }
import scala.util.Try

/**
 * Repository for command records
 */
trait QueryRepository[Session] extends Repository[Session, QueryTemplate, Query] {
  def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit]
  def updateProgress(id: Long, progress: List[Float])(implicit session: Session): Try[Unit]
}

