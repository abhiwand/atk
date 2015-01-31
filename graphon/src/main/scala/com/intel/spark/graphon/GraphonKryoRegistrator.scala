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
