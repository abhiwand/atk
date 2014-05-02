package com.intel.intelanalytics.domain

case class User(id: Long, api_key: String) extends HasId{
  require(id > 0)
  require(api_key != null && !api_key.isEmpty)
}
