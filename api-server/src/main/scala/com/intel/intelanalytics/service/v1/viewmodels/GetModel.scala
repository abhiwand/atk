package com.intel.intelanalytics.service.v1.viewmodels

case class GetModel(id: Long, ia_uri: String, name: String, links: List[RelLink]) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name must not be null")
  require(links != null, "links must not be null")
  require(ia_uri != null, "ia_uri must not be null")
}
