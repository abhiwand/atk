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

import com.typesafe.config.{ ConfigFactory, Config }

import scala.util.Try

/**
 * Encapsulates registration data for `Archive` instances.
 *
 * @param name the name of the Archive (e.g. the name of the jar, without the extension)
 * @param parent the name of the Archive whose class loader should be the parent for this
 *               archive's class loader
 * @param className the name of the class that will manage the archive
 * @param configPath the location of configuration information (in the config tree)
 *                   that applies to the archive
 */
case class ArchiveDefinition(name: String,
                             parent: String,
                             className: String,
                             configPath: String)

/**
 * Companion object, provides methods for constructing ArchiveDefinitions
 */
object ArchiveDefinition {
  /**
   * Constructs an ArchiveDefinition by reading it from a Config object
   * @param archiveName the name of the ArchiveDefinition to load
   * @param config the {Config} object that presumably has the information to load
   * @param defaultParentArchiveName the parent archive to use if no parent is present in the Config object
   * @param configKeyBase the prefix string used to locate the archive definition in the Config object.
   *                      The final key is determined by the configKeyBase + archiveName concatenated.
   */
  def apply(archiveName: String,
            config: Config,
            defaultParentArchiveName: String,
            configKeyBase: String = "intel.analytics.component.archives"): ArchiveDefinition = {
    val configKey = configKeyBase + "." + archiveName
    val restricted = Try {
      config.getConfig(configKey)
    }.getOrElse(
      {
        Archive.logger(s"No config found for '$configKey', using empty")
        ConfigFactory.empty()
      })
    val parent = Try {
      restricted.getString("parent")
    }.getOrElse({
      Archive.logger(s"Using default value ($defaultParentArchiveName) for archive parent")
      defaultParentArchiveName
    })
    val className = Try {
      restricted.getString("class")
    }.getOrElse({
      Archive.logger("No class entry found, using standard DefaultArchive class")
      "com.intel.intelanalytics.component.DefaultArchive"
    })

    val configPath = Try {
      restricted.getString("config-path")
    }.getOrElse(
      {
        Archive.logger("No config-path found, using default")
        configKey
      })
    ArchiveDefinition(archiveName, parent, className, configPath)
  }

}
