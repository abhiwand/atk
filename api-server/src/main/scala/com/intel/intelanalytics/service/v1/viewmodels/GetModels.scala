package com.intel.intelanalytics.service.v1.viewmodels

case class GetModels(id: Long, name: String, url: String) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name must not be null")
  require(url != null)
}
