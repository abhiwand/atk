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

package com.intel.intelanalytics.algorithm

import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.component.Archive
import com.typesafe.config.Config

import scala.reflect.ClassTag

class IGiraphArchive extends Archive {

  val commands: Seq[Class[_]] = Seq(classOf[com.intel.intelanalytics.algorithm.graph.LoopyBeliefPropagation],
    classOf[com.intel.intelanalytics.algorithm.graph.LabelPropagation],
    classOf[com.intel.intelanalytics.algorithm.graph.AlternatingLeastSquares])
  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = descriptor match {
    //TODO: move the plumbing parts to the Archive trait and make this just a simple PartialFunction
    case "CommandPlugin" => commands
      .map(c => load(c.getName))
      .filter(i => i.isInstanceOf[T])
      .map(i => i.asInstanceOf[T])
    case _ => throw new NotFoundException("instances", descriptor)
  }

  /**
   * Called before the application as a whole shuts down. Not guaranteed to be called,
   * nor guaranteed that the application will not shut down while this method is running,
   * though an effort will be made.
   */
  override def stop(): Unit = {

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

  /**
   * Called before processing any requests.
   *
   * @param configuration Configuration information, scoped to that required by the
   *                      plugin based on its installed paths.
   */
  override def start(configuration: Config): Unit = {

  }
}
