package com.intel.intelanalytics.domain.command

/**
 * Post status change to a command
 * @param status status indicate the action to act on command, eg, cancel
 */
case class CommandPost(status: String) {
  require(status.equals("cancel"), s"status $status is not valid")
}
