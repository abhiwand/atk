package com.intel.intelanalytics.domain.query

/**
 * Input Arguments object for querying the records of a dataframe by an offset and count
 * @param id  id of dataframe
 * @param offset number of records to offset the count
 * @param count  number of records to return from dataframe
 * @tparam Identifier Identifer type for the table. Currently only a long is supported.
 */
case class RowQuery[Identifier](id: Identifier, offset: Long, count: Int)