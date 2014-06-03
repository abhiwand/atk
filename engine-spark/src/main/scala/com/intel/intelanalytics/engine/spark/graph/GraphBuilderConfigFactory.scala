package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.graphconstruction._
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.parser.rule.{ Value => GBValue, VertexRule => GBVertexRule, EdgeRule => GBEdgeRule, PropertyRule => GBPropertyRule }

import scala.collection.mutable.ListBuffer
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils
import com.intel.graphbuilder.parser.rule.ConstantValue
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig
import com.intel.graphbuilder.parser.rule.ParsedValue
import com.intel.intelanalytics.domain.graphconstruction.Value

import com.intel.intelanalytics.domain.graphconstruction.OutputConfiguration
import com.intel.graphbuilder.parser.InputSchema
import com.intel.intelanalytics.domain.Schema
import com.intel.intelanalytics.domain.graphconstruction.EdgeRule
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.intelanalytics.domain.graphconstruction.VertexRule
import com.intel.intelanalytics.domain.graphconstruction.Property
import spray.json.JsObject

/**
 * Converter that produces the graphbuilder3 consumable
 * {@code com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig} object from a {@code GraphLoad} command,
 * the schema of the source dataframe, and the metadata of the graph being written to.
 *
 * @param schema Schema of the source dataframe.
 * @param graphLoad The graph loading command.
 * @param graph Metadata for the graph being written to.
 */
class GraphBuilderConfigFactory(val schema: Schema, val graphLoad: GraphLoad[JsObject, Long, Long], graph: Graph) {

  val graphConfig: GraphBuilderConfig = {
    new GraphBuilderConfig(getInputSchema(schema),
      getGBVertexRules(graphLoad.vertexRules),
      getGBEdgeRules(graphLoad.edgeRules),
      getTitanConfiguration(graph.name, graphLoad.outputConfig),
      biDirectional = graphLoad.bidirectional,
      append = false,
      retainDanglingEdges = graphLoad.retainDanglingEdges,
      inferSchema = true,
      broadcastVertexIds = false)
  }

  /**
   * Converts {@code com.intel.intelanalytics.domain.Schema} into {@code com.intel.graphbuilder.parser.InputSchema}
   * @param schema The dataframe schema to be converted.
   * @return
   */
  private def getInputSchema(schema: Schema): InputSchema = {
    val columns = new ListBuffer[ColumnDef]()

    for ((string, dataType) <- schema.columns) {
      val column = new ColumnDef(string, dataType.scalaType)
      columns += column
    }

    new InputSchema(columns)
  }

  /**
   * Produces graphbuilder3 consumable {@code com.intel.graphbuilder.util.SerializableBaseConfiguration} from
   * a graph name and a {@code com.intel.intelanalytics.domain.graphconstruction.outputConfiguration}
   * @param graphName Name of the graph to be written to.
   * @param outputConfiguration Output configuration from engine command.
   * @return GraphBuilder3 consumable com.intel.graphbuilder.util.SerializableBaseConfiguration
   */
  private def getTitanConfiguration(graphName: String, outputConfiguration: OutputConfiguration): SerializableBaseConfiguration = {

    // Only use this method when the store is Titan
    require(outputConfiguration.storeName == "Titan")

    var titanConfiguration = new SerializableBaseConfiguration

    titanConfiguration.setProperty("storage.backend", "hbase")
    titanConfiguration.setProperty("storage.tablename", ConvertGraphUserNameToBackendName(graphName))

    titanConfiguration.setProperty("storage.hostname", ExamplesUtils.storageHostname)
    titanConfiguration.setProperty("storage.batch-loading", "true")
    titanConfiguration.setProperty("autotype", "none")
    titanConfiguration.setProperty("storage.buffer-size", "2048")
    titanConfiguration.setProperty("storage.attempt-wait", "300")
    titanConfiguration.setProperty("storage.lock-wait-time", "400")
    titanConfiguration.setProperty("storage.lock-retries", "15")
    titanConfiguration.setProperty("storage.idauthority-retries", "30")
    titanConfiguration.setProperty("storage.read-attempts", "6")
    titanConfiguration.setProperty("ids.block-size", "300000")
    titanConfiguration.setProperty("ids.renew-timeout", "150000")

    for ((key, value) <- outputConfiguration.configuration) {
      titanConfiguration.addProperty(key, value)
    }

    titanConfiguration
  }

  /**
   * Converts {@code  com.intel.intelanalytics.domain.graphconstruction.Value} into the GraphBuilder3 consumable
   * {@code com.intel.graphbuilder.parser.rule.Value}
   *
   * @param value A value from a graph load's parsing rules.
   * @return A com.intel.graphbuilder.parser.rule.Value
   */
  private def getGBValue(value: Value): GBValue = {
    if (value.source == GBValueSourcing.CONSTANT) {
      new ConstantValue(value.value)
    }
    else {
      new ParsedValue(value.value)
    }
  }

  /**
   * Converts {com.intel.intelanalytics.domain.graphconstruction.Property} into the GraphBuilder3 consumable
   * {@code com.intel.graphbuilder.parser.rule.PropertyRule}
   * @param property A property rule from a graph load's parsing rules.
   * @return A com.intel.graphbuilder.parser.rule.PropertyRule
   */
  private def getGBPropertyRule(property: Property): GBPropertyRule = {
    new GBPropertyRule(getGBValue(property.key), getGBValue(property.value))
  }

  /**
   * Converts {@code com.intel.intelanalytics.domain.graphconstruction.VertexRule} to GraphBuilder3 consumable
   * {@code com.intel.graphbuilder.parser.rule.VertexRule }
   * @param vertexRule A vertex rule from a graph load's parsing rules.
   * @return A com.intel.intelanalytics.domain.graphconstruction.VertexRule
   */
  private def getGBVertexRule(vertexRule: VertexRule): GBVertexRule = {
    new GBVertexRule(getGBPropertyRule(vertexRule.id), (vertexRule.properties map getGBPropertyRule))
  }

  /**
   * Converts a list of {@code com.intel.intelanalytics.domain.graphconstruction.VertexRule}'s into a list of
   * GraphBuilder3 consumable {@code com.intel.graphbuilder.parser.rule.VertexRule}'s
   * @param vertexRules A list of vertex rules from a graph load's parsing rules.
   * @return A list of com.intel.intelanalytics.domain.graphconstruction.VertexRule
   */
  private def getGBVertexRules(vertexRules: List[VertexRule]): List[GBVertexRule] = {
    vertexRules map getGBVertexRule
  }

  /**
   * Converts {@code com.intel.intelanalytics.domain.graphconstruction.EdgeRule} to GraphBuilder3 consumable
   * {@code com.intel.graphbuilder.parser.rule.EdgeRule }
   * @param edgeRule An edge rule from a graph load's parsing rules.
   * @return A com.intel.intelanalytics.domain.graphconstruction.EdgeRule
   */
  private def getGBEdgeRule(edgeRule: EdgeRule): GBEdgeRule = {
    new GBEdgeRule(getGBPropertyRule(edgeRule.tail), getGBPropertyRule(edgeRule.head),
      getGBValue(edgeRule.label), (edgeRule.properties map getGBPropertyRule))
  }

  /**
   * Converts a list of {@code com.intel.intelanalytics.domain.graphconstruction.EdgeRule}'s into a list of
   * GraphBuilder3 consumable {@code com.intel.graphbuilder.parser.rule.EdgeRule}'s
   * @param edgeRules A list of edge rules from a graph load's parsing rules.
   * @return A list of com.intel.intelanalytics.domain.graphconstruction.EdgeRule
   */
  private def getGBEdgeRules(edgeRules: List[EdgeRule]): List[GBEdgeRule] = {
    edgeRules map getGBEdgeRule
  }

}
