package com.intel.intelanalytics.domain

case class DataFrameTemplate(name: String) {
  require(name != null)
  require(name.trim.length > 0)
}

case class DataFrame(id: Long, name: String, schema: Schema = Schema()) extends HasId {
  require(id > 0)
  require(name != null)
  require(name.trim.length > 0)
}
