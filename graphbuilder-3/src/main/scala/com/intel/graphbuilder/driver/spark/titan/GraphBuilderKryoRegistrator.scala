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

package com.intel.graphbuilder.driver.spark.titan

import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.io.NullWritable
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
import com.intel.graphbuilder.elements.GBVertex
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.rule.PropertyRuleParser
import com.intel.graphbuilder.elements.GBEdge
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
    kryo.register(classOf[GBEdge])
    kryo.register(classOf[GbIdToPhysicalId])
    kryo.register(classOf[GraphElement])
    kryo.register(classOf[Property])
    kryo.register(classOf[GBVertex])

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
    kryo.register(classOf[FaunusVertex], new FaunusVertexSerializer())
    kryo.register(classOf[NullWritable])
    // avoid Spark top(n) issue with Kryo serializer:
    //    kryo.register(classOf[org.apache.spark.util.BoundedPriorityQueue[(Double, Vertex)]])
  }
}
