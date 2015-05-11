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

import com.typesafe.config.{ ConfigResolveOptions, ConfigFactory, Config }

import scala.collection.JavaConverters._

/**
 * The configuration for an application environment, typically managed globally by {Archive}
 */
class SystemConfig(val rootConfiguration: Config = ConfigFactory.load(SystemConfig.getClass.getClassLoader,
                     //Allow unresolved subs because user may have specified subs on the command line
                     //that can't be resolved yet until other archives are loaded
                     ConfigResolveOptions.defaults().setAllowUnresolved(true))) {

  def extraClassPath(archivePath: String): Array[String] = {
    Archive.logger(s"Checking archive path $archivePath for extra classpath")
    val path = archivePath + ".extra-classpath"
    val result = getStrings(path)
    Archive.logger(s"Checking archive path $archivePath for extra classpath - result: [${result.mkString(", ")}]")
    result
  }

  def extraArchives(archivePath: String): Array[String] = {
    val path = archivePath + ".extra-archives"
    getStrings(path)
  }

  def getStrings(path: String): Array[String] = {
    if (rootConfiguration.hasPath(path)) {
      rootConfiguration.getStringList(path).asScala.toArray
    }
    else {
      Array.empty
    }
  }

  val defaultParentArchiveName: String = rootConfiguration.getString(SystemConfig.defaultParentArchiveKey)

  val debugConfig = rootConfiguration.getBoolean(SystemConfig.debugConfigKey)

  val debugConfigFolder = rootConfiguration.getString(SystemConfig.debugConfigPrefix) + java.util.UUID.randomUUID.toString + "/"

  val jarFolders = rootConfiguration.getStringList(SystemConfig.jarFolders).asScala.toArray

  val sourceRoots = rootConfiguration.getStringList(SystemConfig.sourceRoots).asScala.toArray

}

object SystemConfig {

  private[component] val debugConfigKey: String = "intel.analytics.launcher.debug-config.enabled"

  private[component] val debugConfigPrefix: String = "intel.analytics.launcher.debug-config.prefix"

  private[component] val defaultParentArchiveKey = "intel.analytics.launcher.default-parent-archive"

  private[component] val jarFolders = "intel.analytics.launcher.jar-folders"

  private[component] val sourceRoots = "intel.analytics.launcher.source-roots"

}
