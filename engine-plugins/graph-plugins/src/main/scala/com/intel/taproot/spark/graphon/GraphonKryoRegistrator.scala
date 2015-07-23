/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.spark.graphon

import com.intel.taproot.analytics.engine.EngineKryoRegistrator
import com.intel.taproot.spark.graphon.beliefpropagation.VertexState
import com.intel.taproot.spark.graphon.iatpregel._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.graphx.GraphKryoRegistrator

/**
 * Register GraphOn classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * \ * <p>
 * Usage:
 * conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 * conf.set("spark.kryo.registrator", "com.intel.taproot.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
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