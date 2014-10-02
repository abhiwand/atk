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

import com.intel.intelanalytics.component.{ ClassLoaderAware, Archive }

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import com.intel.event.EventLogging

class EngineApplication extends Archive with EventLogging with ClassLoaderAware {

  var engine: EngineComponent with FrameComponent[_, _] with CommandComponent = null

  override def getAll[T: ClassTag](descriptor: String) = {
    descriptor match {
      case "engine" => Seq(engine.engine.asInstanceOf[T])
      case _ => Seq()
    }
  }

  override def stop() = {
    info("Shutting down engine")
    engine.engine.shutdown
  }

  override def start() = {

    try {
      //TODO: when Engine moves to its own process, it will need to start its own Akka actor system.
      engine = com.intel.intelanalytics.component.Boot.getArchive("engine-spark")
        .load("com.intel.intelanalytics.engine.spark.SparkComponent")
        .asInstanceOf[EngineComponent with FrameComponent[_, _] with CommandComponent]

    }
    catch {
      case NonFatal(e) =>
        error("An error occurred while starting the engine.", exception = e)
        throw e
    }
  }

}
