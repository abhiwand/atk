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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.ClassLoaderAware
import com.intel.intelanalytics.component.{Locator, Component}
import com.intel.intelanalytics.shared.EventLogging
import com.typesafe.config.Config

import scala.util.control.NonFatal

class EngineApplication extends Component with EventLogging with ClassLoaderAware with Locator {

  var engine: EngineComponent with FrameComponent with CommandComponent = null

  def get[T](descriptor: String) = {
    descriptor match {
      case "engine" => Some(engine.engine.asInstanceOf[T])
      case _ => None
    }
  }

  def stop() = {
    info("Shutting down engine")
    engine.engine.shutdown
  }

  def start(configuration: Config) = {

    try {
      //TODO: when Engine moves to its own process, it will need to start its own Akka actor system.

      val sparkLoader = {
        com.intel.intelanalytics.component.Boot.getClassLoader("engine-spark")
      }

      engine = {
        withLoader(sparkLoader) {
          val class_ = sparkLoader.loadClass("com.intel.intelanalytics.engine.spark.SparkComponent")
          val instance = class_.newInstance()
          instance.asInstanceOf[EngineComponent with FrameComponent with CommandComponent]
        }
      }
    }
    catch {
      case NonFatal(e) =>
        error("An error occurred while starting the engine.", exception = e)
        throw e
    }
  }

  /**
   * The location at which this component should be installed in the component
   * tree. For example, a graph machine learning algorithm called Loopy Belief
   * Propagation might wish to be installed at
   * "commands/graphs/ml/loopy_belief_propagation". However, it might not actually
   * get installed there if the system has been configured to override that
   * default placement.
   */
  override def defaultLocation: String = "engine"
}
