package com.intel.graphbuilder.driver.spark.titan

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.graph.GraphConnector
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser._
import com.intel.graphbuilder.parser.rule._

/**
 * Register GraphBuilder classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * <p>
 * Kryo is 2x to 10x faster than Java Serialization.  Kryo was 2 hours
 * faster with 23GB of Netflix data.
 * </p>
 * <p>
 *  Usage:
 *   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 *   conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
 * </p>
 */
class GraphBuilderKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {

    // elements package
    kryo.register(classOf[Edge])
    kryo.register(classOf[GbIdToPhysicalId])
    kryo.register(classOf[GraphElement])
    kryo.register(classOf[Property])
    kryo.register(classOf[Vertex])

    // graph package
    kryo.register(classOf[TitanGraphConnector])
    kryo.register(classOf[GraphConnector])

    // parser.rule package
    kryo.register(classOf[DataTypeResolver])
    kryo.register(classOf[EdgeRuleParser])
    kryo.register(classOf[SingleEdgeRuleParser])
    kryo.register(classOf[ParseRule])
    kryo.register(classOf[EdgeRule])
    kryo.register(classOf[VertexRule])
    kryo.register(classOf[PropertyRule])
    kryo.register(classOf[PropertyRuleParser])
    kryo.register(classOf[SinglePropertyRuleParser])
    kryo.register(classOf[Value])
    kryo.register(classOf[CompoundValue])
    kryo.register(classOf[ConstantValue])
    kryo.register(classOf[ParsedValue])
    kryo.register(classOf[VertexRuleParser])
    kryo.register(classOf[SingleVertexRuleParser])

    // parser package
    kryo.register(classOf[CombinedParser])
    kryo.register(classOf[InputRow])
    kryo.register(classOf[InputSchema])
    kryo.register(classOf[ColumnDef])
  }
}
