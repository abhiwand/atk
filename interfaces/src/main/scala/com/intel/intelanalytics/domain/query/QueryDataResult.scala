package com.intel.intelanalytics.domain.query

import com.intel.intelanalytics.domain.schema.Schema

/**
 * Result returned by query
 * @param data data from the query
 * @param schema schema to describe the data
 */
case class QueryDataResult(data: Iterable[Array[Any]], schema: Option[Schema])
