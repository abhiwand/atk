package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.schema.Schema

/**
 * Class containing the list of schema objects corresponding
 */
case class SchemaList(schemas: List[Schema]) {
  require(schemas != null, "schemas cannot be null")
}

