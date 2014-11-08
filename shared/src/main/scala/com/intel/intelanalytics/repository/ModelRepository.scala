package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.model.{ ModelTemplate, Model }

/**
 * Repository for models
 */
trait ModelRepository[Session] extends Repository[Session, ModelTemplate, Model] with NameableRepository[Session, Model] {

  /**
   * Return all the models
   * @param session current session
   * @return all the models
   */
  def scanAll()(implicit session: Session): Seq[Model]

}
