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

package com.intel.spark.graphon

import com.intel.intelanalytics.engine.spark.EngineKryoRegistrator
import com.intel.spark.graphon.beliefpropagation.VertexState
import com.intel.spark.graphon.iatpregel._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.graphx.GraphKryoRegistrator

/**
 * Register GraphOn classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * \ * <p>
 * Usage:
 * conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 * conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
 * </p>
 */
class GraphonKryoRegistrator extends EngineKryoRegistrator {

  override def registerClasses(kryo: Kryo) {

    // IATPregel Logging Classes

    kryo.register(classOf[SuperStepNetDelta])

    // Belief propagation classes
    kryo.register(classOf[VertexState])
    kryo.register(classOf[BasicCountsInitialReport[VertexState, Double]])

    // GraphX classes
    val graphXRegistrar = new GraphKryoRegistrator()
    graphXRegistrar.registerClasses(kryo)
  }
}
