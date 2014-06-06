package com.intel.intelanalytics.domain

import org.joda.time.DateTime

/**
 * Users of the system.
 *
 * @param id unique id auto-generated by the database
 * @param username unique name identifying this user
 * @param api_key API key used to authenticate this user
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class User(id: Long, username: Option[String], api_key: String, createdOn: DateTime, modifiedOn: DateTime ) extends HasId {
  require(id >= 0)
  require(api_key != null && !api_key.isEmpty)
}

case class UserTemplate(api_key: String) {
  require(api_key != null && !api_key.isEmpty)
}
