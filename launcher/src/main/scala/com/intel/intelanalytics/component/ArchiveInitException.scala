package com.intel.intelanalytics.component

/**
 * Generic exception during initialization of an Archive
 */
class ArchiveInitException(message: String, e: Exception) extends RuntimeException(message + " " + e.getMessage, e) {

}
