package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.HasId

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
