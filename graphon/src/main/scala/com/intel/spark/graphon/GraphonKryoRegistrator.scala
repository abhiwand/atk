package com.intel.spark.graphon

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.intel.spark.graphon.iatpregel._
import com.intel.spark.graphon.beliefpropagation.VertexState

/**
 * Register GraphOn classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * \ * <p>
 *  Usage:
 *   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 *   conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
 * </p>
 */
class GraphonKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {

    // IATPregel Logging Classes

    kryo.register(classOf[SuperStepCountNetDelta])

    // Belief propagation classes
    kryo.register(classOf[VertexState])
    kryo.register(classOf[BasicCountsInitialReport[VertexState, Double]])
  }
}
