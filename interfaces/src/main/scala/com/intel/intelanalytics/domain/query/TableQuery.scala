package com.intel.intelanalytics.domain.query

/**
 * Created by rhicke on 7/3/14.
 */
case class TableQuery[Identifier](id: Identifier, offset: Long, count: Int) {

}
