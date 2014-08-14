package com.intel.intelanalytics.domain.query

import com.intel.intelanalytics.domain.query.{ Execution => QueryExecution }
import com.intel.intelanalytics.domain.schema.Schema

case class PagedQueryResult(execution: QueryExecution, schema: Option[Schema])
