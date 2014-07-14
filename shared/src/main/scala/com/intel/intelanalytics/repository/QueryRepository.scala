package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.query.{ QueryTemplate, Query }
import com.intel.intelanalytics.engine.ProgressInfo
import scala.util.Try

/**
 * Repository for command records
 */
trait QueryRepository[Session] extends Repository[Session, QueryTemplate, Query] {
  def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit]
  def updateProgress(id: Long, progress: List[Float], detailedProgress: List[ProgressInfo])(implicit session: Session): Try[Unit]
}

