package com.intel.intelanalytics.domain.command

/**
 * Action to act on command
 * @param status status indicate the action to act on command, eg, cancel
 */
case class CommandAction(status: String)
