//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import com.typesafe.config.Config

/**
 * The state for an application environment, typically managed globally by {Archive}
 */
class SystemConfig(val rootConfiguration: Config, archives: Map[String, Archive] = Map.empty) {

  /**
   * Creates a copy of this system configuration with the given root Config.
   */
  def withRootConfiguration(config: Config): SystemConfig = new SystemConfig(config, archives)

  val debugConfig = rootConfiguration.getBoolean(SystemConfig.debugConfigKey)

  /**
   * Look up the classloader for a given archive. Convenience method.
   */
  def loader(archiveName: String) = {
    require(archiveName != null, "archiveName cannot be null")
    archive(archiveName).map(_.classLoader)
  }

  /**
   * Look up an archive by name
   */
  def archive(archiveName: String) = {
    require(archiveName != null, "archiveName cannot be null")
    archives.get(archiveName)
  }

  /**
   * Generate a new system configuration with an additional archive included
   */
  def addArchive(archive: Archive) = {
    require(archive != null, "archive cannot be null")

    new SystemConfig(rootConfiguration, archives + (archive.definition.name -> archive))
  }

}

object SystemConfig {
  private[component] val debugConfigKey: String = "intel.analytics.launcher.debug-config"

}
