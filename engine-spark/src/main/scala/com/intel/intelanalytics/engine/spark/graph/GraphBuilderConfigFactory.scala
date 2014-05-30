package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.graphconstruction._
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.parser.rule.{ Value => GBValue, VertexRule => GBVertexRule, EdgeRule => GBEdgeRule, PropertyRule => GBPropertyRule }
import com.intel.graphbuilder.parser.rule.ConstantValue
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig
import com.intel.graphbuilder.parser.rule.ParsedValue
import com.intel.intelanalytics.domain.graphconstruction.ValueRule

import com.intel.intelanalytics.domain.graphconstruction.OutputConfiguration
import com.intel.graphbuilder.parser.InputSchema
import com.intel.intelanalytics.domain.Schema
import com.intel.intelanalytics.domain.graphconstruction.EdgeRule
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.intelanalytics.domain.graphconstruction.VertexRule
import com.intel.intelanalytics.domain.graphconstruction.PropertyRule
import spray.json.JsObject
import com.intel.intelanalytics.domain.DataTypes.DataType
import com.typesafe.config.ConfigFactory

/**
 * Converter that produces the graphbuilder3 consumable
 * com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig object from a GraphLoad command,
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
   * Converts com.intel.intelanalytics.domain.Schema into com.intel.graphbuilder.parser.InputSchema
   * @param schema The dataframe schema to be converted.
   * @return Dataframe schema as a com.intel.graphbuilder.parser.InputSchema
   */
  private def getInputSchema(schema: Schema): InputSchema = {

    val columns: List[ColumnDef] = schema.columns map
      { case (name: String, dataType: DataType) => new ColumnDef(name, dataType.scalaType) }

    new InputSchema(columns)
  }

  /**
   * Produces graphbuilder3 consumable com.intel.graphbuilder.util.SerializableBaseConfiguration from
   * a graph name and a com.intel.intelanalytics.domain.graphconstruction.outputConfiguration
   * @param graphName Name of the graph to be written to.
   * @param outputConfiguration Output configuration from engine command.
   * @return GraphBuilder3 consumable com.intel.graphbuilder.util.SerializableBaseConfiguration
   */
  private def getTitanConfiguration(graphName: String, outputConfiguration: OutputConfiguration): SerializableBaseConfiguration = {

    // Only use this method when the store is Titan
    require(outputConfiguration.storeName.equalsIgnoreCase("Titan"))

    var titanConfiguration = new SerializableBaseConfiguration

    val engineConfiguration = ConfigFactory.load("engine.conf").getConfig("engine.graphbuilder.load")

    //These parameters are set from engine config values

    // TODO: hardwire defaults, should a key not be present in the conf file

    val mapping = Seq(
      "storage.backend" -> "storage.backend",
      "storage.hostname" -> "storage.hostname",
      "storage.batch-loading" -> "storage.batch-loading",
      "autotype" -> "autotype",
      "storage.buffer-size" -> "storage.buffer-size",
      "storage.attempt-wait" -> "storage.attempt-wait",
      "storage.lock-wait-time" -> "storage.lock-wait-time",
      "storage.lock-retries" -> "storage.lock-retires",
      "storage.idauthority-retries" -> "storage.idauthority-retries",
      "storage.read-attempts" -> "storage.read-attempts",
      "ids.block-size" -> "ids.block-size",
      "ids.renew-timeout" -> "ids.renew-timeout")

    for ((k, v) <- mapping) {
      titanConfiguration.setProperty(v, engineConfiguration.getString(k))
    }

    titanConfiguration.setProperty("storage.tablename", GraphName.convertGraphUserNameToBackendName(graphName))

    for ((key, value) <- outputConfiguration.configuration) {
      titanConfiguration.addProperty(key, value)
    }

    titanConfiguration
  }

  /**
   * Converts com.intel.intelanalytics.domain.graphconstruction.Value into the GraphBuilder3 consumable
   * com.intel.graphbuilder.parser.rule.Value
   *
   * @param value A value from a graph load's parsing rules.
   * @return A com.intel.graphbuilder.parser.rule.Value
   */
  private def getGBValue(value: ValueRule): GBValue = {
    if (value.source == GBValueSourcing.CONSTANT) {
      new ConstantValue(value.value)
    }
    else {
      new ParsedValue(value.value)
    }
  }

  /**
   * Converts {com.intel.intelanalytics.domain.graphconstruction.Property} into the GraphBuilder3 consumable
   * com.intel.graphbuilder.parser.rule.PropertyRule
   * @param property A property rule from a graph load's parsing rules.
   * @return A com.intel.graphbuilder.parser.rule.PropertyRule
   */
  private def getGBPropertyRule(property: PropertyRule): GBPropertyRule = {
    new GBPropertyRule(getGBValue(property.key), getGBValue(property.value))
  }

  /**
   * Converts com.intel.intelanalytics.domain.graphconstruction.VertexRule to GraphBuilder3 consumable
   * com.intel.graphbuilder.parser.rule.VertexRule
   * @param vertexRule A vertex rule from a graph load's parsing rules.
   * @return A com.intel.intelanalytics.domain.graphconstruction.VertexRule
   */
  private def getGBVertexRule(vertexRule: VertexRule): GBVertexRule = {
    new GBVertexRule(getGBPropertyRule(vertexRule.id), (vertexRule.properties map getGBPropertyRule))
  }

  /**
   * Converts a list of com.intel.intelanalytics.domain.graphconstruction.VertexRule's into a list of
   * GraphBuilder3 consumable com.intel.graphbuilder.parser.rule.VertexRule's
   * @param vertexRules A list of vertex rules from a graph load's parsing rules.
   * @return A list of com.intel.intelanalytics.domain.graphconstruction.VertexRule
   */
  private def getGBVertexRules(vertexRules: List[VertexRule]): List[GBVertexRule] = {
    vertexRules map getGBVertexRule
  }

  /**
   * Converts com.intel.intelanalytics.domain.graphconstruction.EdgeRule to GraphBuilder3 consumable
   * com.intel.graphbuilder.parser.rule.EdgeRule
   * @param edgeRule An edge rule from a graph load's parsing rules.
   * @return A com.intel.intelanalytics.domain.graphconstruction.EdgeRule
   */
  private def getGBEdgeRule(edgeRule: EdgeRule): GBEdgeRule = {
    new GBEdgeRule(getGBPropertyRule(edgeRule.tail), getGBPropertyRule(edgeRule.head),
      getGBValue(edgeRule.label), (edgeRule.properties map getGBPropertyRule))
  }

  /**
   * Converts a list of com.intel.intelanalytics.domain.graphconstruction.EdgeRule's into a list of
   * GraphBuilder3 consumable com.intel.graphbuilder.parser.rule.EdgeRule's
   * @param edgeRules A list of edge rules from a graph load's parsing rules.
   * @return A list of com.intel.intelanalytics.domain.graphconstruction.EdgeRule
   */
  private def getGBEdgeRules(edgeRules: List[EdgeRule]): List[GBEdgeRule] = {
    edgeRules map getGBEdgeRule
  }

}
