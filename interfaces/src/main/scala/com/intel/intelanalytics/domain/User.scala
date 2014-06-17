package com.intel.intelanalytics.domain

import org.joda.time.DateTime


/**
 * Users of the system.
 *
 * @param id unique id auto-generated by the database
 * @param username unique name identifying this user
 * @param apiKey API key used to authenticate this user, None means user is disabled.
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class User(id: Long,
                username: Option[String],
                apiKey: Option[String],
                createdOn: DateTime,
                modifiedOn: DateTime ) extends HasId {
  require(id >= 0)
  require(apiKey != null && !apiKey.isEmpty)
}

