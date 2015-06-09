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

package com.intel.intelanalytics.engine.spark

import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetrics
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRdd, MiscFrameFunctions }
import org.apache.spark.frame.FrameRdd

import org.apache.spark.serializer.KryoRegistrator

/**
 * Register classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * <p>
 * Kryo is 2x to 10x faster than Java Serialization.  In one experiment,
 * with graph building Kryo was 2 hours faster with 23GB of Netflix data.
 * </p>
 * <p>
 * Usage:
 * conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 * conf.set("spark.kryo.registrator", "com.intel.intelanalytics.engine.spark.EngineKryoRegistrator")
 * </p>
 */
class EngineKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    // frame related classes
    kryo.register(classOf[Row])
    kryo.register(classOf[Schema])
    kryo.register(classOf[LegacyFrameRdd])
    kryo.register(classOf[FrameRdd])
    kryo.register(ClassificationMetrics.getClass)
    kryo.register(MiscFrameFunctions.getClass)

    // register GraphBuilder classes
    val gbRegistrator = new GraphBuilderKryoRegistrator()
    gbRegistrator.registerClasses(kryo)
  }
}
