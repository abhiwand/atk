package com.intel.intelanalytics.domain

import org.joda.time.DateTime

/**
 * Hooks are registered callbacks.
 *
 * A hook allows a client to ask to be notified when a Command is completed.
 *
 * @param id unique id auto-generated by the database for this hook
 * @param commandId the uniqueId for a Command that this hook is a callback for
 * @param uri the URI to POST to when the Command has completed
 * @param completed true if this hook has completed (either successful or done retrying in case of failure)
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class Hook(id: Long, commandId: Long, uri: String, completed: Boolean, createdOn: DateTime, modifiedOn: DateTime) extends HasId {

}
