package com.intel.graphbuilder.driver.spark.titan

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.graph.GraphConnector
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser._
import com.intel.graphbuilder.parser.rule._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import com.intel.graphbuilder.schema._
import com.intel.graphbuilder.parser.rule.ParsedValue
import com.intel.graphbuilder.parser.rule.SingleEdgeRuleParser
import com.intel.graphbuilder.schema.EdgeLabelDef
import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.rule.PropertyRuleParser
import com.intel.graphbuilder.elements.Edge
import com.intel.graphbuilder.parser.rule.SinglePropertyRuleParser
import com.intel.graphbuilder.parser.rule.SingleVertexRuleParser
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.schema.PropertyDef
import com.intel.graphbuilder.parser.rule.VertexRuleParser
import com.intel.graphbuilder.elements.GbIdToPhysicalId
import com.intel.graphbuilder.parser.rule.EdgeRuleParser
import com.intel.graphbuilder.parser.rule.CompoundValue
import com.intel.graphbuilder.parser.rule.ConstantValue
import com.intel.graphbuilder.parser.rule.EdgeRule
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.graphbuilder.parser.rule.VertexRule

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

    // reader package
    kryo.register(classOf[ImmutableBytesWritable])
    kryo.register(classOf[Result])

    // schema package
    kryo.register(classOf[EdgeLabelDef])
    kryo.register(classOf[PropertyDef])
    kryo.register(classOf[PropertyType.Value])
    kryo.register(classOf[GraphSchema])
    kryo.register(classOf[InferSchemaFromData])
    kryo.register(classOf[SchemaAccumulableParam])
  }
}
