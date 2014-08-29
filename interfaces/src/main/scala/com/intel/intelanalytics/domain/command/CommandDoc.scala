package com.intel.intelanalytics.domain.command

/**
 * Documentation for a command
 * @param oneLineSummary - one line summary of the command
 * @param extendedSummary - more detailed summary of the command
 */
case class CommandDoc(oneLineSummary: String, extendedSummary: Option[String] = None)
// TODO - add 'errors', 'notes', 'examples', 'version'
// (arguments and return are documented individually)
