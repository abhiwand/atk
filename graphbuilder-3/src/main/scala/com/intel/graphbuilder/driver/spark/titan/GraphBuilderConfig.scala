package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.parser.rule.{EdgeRule, VertexRule}
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import org.apache.commons.configuration.Configuration

/**
 * Configuration options for GraphBuilder
 *
 * @param inputSchema describes the columns of input
 * @param vertexRules rules for parsing Vertices
 * @param edgeRules rules for parsing Edges
 * @param titanConfig connect to Titan
 * @param biDirectional true to create an Edge in the opposite direction for each one parsed
 * @param append true to append to an existing Graph, slower because each write requires a read.
 * @param retainDanglingEdges true to add extra vertices for dangling edges, false to drop dangling edges
 * @param inferSchema true to automatically infer the schema from the rules and, if needed, the data, false if the schema is already defined.
 */
case class GraphBuilderConfig(inputSchema: InputSchema,
                              vertexRules: List[VertexRule],
                              edgeRules: List[EdgeRule],
                              titanConfig: SerializableBaseConfiguration,
                              biDirectional: Boolean = false,
                              append: Boolean = false,
                              retainDanglingEdges: Boolean = false,
                              inferSchema: Boolean = true) extends Serializable {

}