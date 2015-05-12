//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.component

/**
 * Entry point for all Intel Analytics Toolkit applications.
 *
 * Manages a registry of plugins.
 *
 */
object Boot {

  /**
   * Returns the requested archive, loading it if needed.
   * @param archiveName the name of the archive
   * @param className the name of the class managing the archive
   *
   * @return the requested archive
   */
  def getArchive(archiveName: String, className: Option[String] = None): Archive = {
    Archive.getArchive(archiveName, className)
  }

  /**
   * Returns the class loader for the given archive
   */
  def getClassLoader(archive: String): ClassLoader = {
    getArchive(archive).classLoader
  }

  def usage() = println("Usage: java -jar launcher.jar <archive> <application>")

  def main(args: Array[String]) = {
    if (args.length < 1 || args.length > 2) {
      usage()
    }
    else {
      try {
        val archiveName: String = args(0)
        val applicationName: Option[String] = { if (args.length == 2) Some(args(1)) else None }
        println("Starting application")
        Archive.logger(s"Starting $archiveName")
        val instance = getArchive(archiveName, applicationName)
        Archive.logger(s"Started $archiveName with ${instance.definition}")
      }
      catch {
        case e: Throwable =>
          var current = e
          while (current != null) {
            Archive.logger(current.toString)
            println(current)
            current.printStackTrace()
            current = current.getCause
          }
      }
    }
  }
}
