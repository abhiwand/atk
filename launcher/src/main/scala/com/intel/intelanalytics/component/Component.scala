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

import com.intel.intelanalytics.component.Boot.ArchiveDefinition
import com.typesafe.config.Config

/**
 * Base interface for a component / plugin.
 */
trait Component {

  private var configuredName: Option[String] = None

  private var config: Option[Config] = None

  /**
   * A component's name, useful for error messages
   */
  final def componentName: String = {
    configuredName.getOrElse(throw new Exception("This component has not been initialized, so it does not have a name"))
  }

  final def configuration: Config = {
    config.getOrElse(throw new Exception("This component has not been initialized, so it does not have a name"))
  }

  /**
   * Called before processing any requests.
   *
   * @param name          the name that was used to locate this component
   * @param configuration Configuration information, scoped to that required by the
   *                      plugin based on its installed paths.
   */
  final def init(name: String, configuration: Config) = {
    configuredName = Some(name)
    config = Some(configuration)
  }

  /**
   * Called before processing any requests.
   *
   */
  def start() = {
  }

  /**
   * Called before the application as a whole shuts down. Not guaranteed to be called,
   * nor guaranteed that the application will not shut down while this method is running,
   * though an effort will be made.
   */
  def stop() = {}
}

