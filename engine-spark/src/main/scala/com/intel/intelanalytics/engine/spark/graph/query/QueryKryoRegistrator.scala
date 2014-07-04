package com.intel.intelanalytics.engine.spark.graph.query

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.intel.intelanalytics.engine.spark.graph.query.roc._
import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.graph.GraphConnector
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser._
import com.intel.graphbuilder.parser.rule._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator

/**
 * Register GraphBuilder classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * <p>
 * Kryo is 2x to 10x faster than Java Serialization.  In one experiment,
 * Kryo was 2 hours faster with 23GB of Netflix data.
 * </p>
 * <p>
 *  Usage:
 *   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 *   conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
 * </p>
 */
class QueryKryoRegistrator extends GraphBuilderKryoRegistrator {

  override def registerClasses(kryo: Kryo) {

    super.registerClasses(kryo)
    //kryo.register(classOf[FeatureProbability])
    //kryo.register(classOf[HistogramRocQuery])

  }
}
