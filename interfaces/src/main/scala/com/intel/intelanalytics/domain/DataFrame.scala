package com.intel.intelanalytics.domain


case class DataFrame(id: Option[Long], name: String, schema: Schema) extends HasId {
  require(id.isEmpty || id.get > 0)
  require(name != null)
  require(name.trim.length > 0)
  require(schema != null)
  require(schema.columns.length > 0)
}


