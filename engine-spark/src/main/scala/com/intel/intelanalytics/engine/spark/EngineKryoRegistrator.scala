package com.intel.intelanalytics.engine.spark

import com.intel.graphbuilder.elements.Vertex
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, RowParseResult }

/**
 * Register classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * <p>
 * Kryo is 2x to 10x faster than Java Serialization.  In one experiment,
 * with graph building Kryo was 2 hours faster with 23GB of Netflix data.
 * </p>
 * <p>
 *  Usage:
 *   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 *   conf.set("spark.kryo.registrator", "com.intel.intelanalytics.engine.spark.EngineKryoRegistrator")
 * </p>
 */
class EngineKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    // frame related classes
    kryo.register(classOf[Row])
    kryo.register(classOf[RowParseResult])
    kryo.register(classOf[FrameRDD])

    // avoid Spark top(n) issue with Kryo serializer:
    kryo.register(classOf[org.apache.spark.util.BoundedPriorityQueue[(Double, Vertex)]])

    // register GraphBuilder classes
    val gbRegistrator = new GraphBuilderKryoRegistrator()
    gbRegistrator.registerClasses(kryo)
  }
}
