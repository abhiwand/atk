package com.intel.intelanalytics.engine.spark.graphbuilder

import com.intel.intelanalytics.domain.{ Schema, GraphTemplate }
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig
import com.intel.intelanalytics.domain.graphconstruction._
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.parser.{ ColumnDef, InputSchema }
import com.intel.graphbuilder.parser.rule.{ Value => GBValue, VertexRule => GBVertexRule, EdgeRule => GBEdgeRule, PropertyRule => GBPropertyRule, ParsedValue, ConstantValue }

import scala.collection.mutable.ListBuffer
import com.intel.graphbuilder.parser.rule.ParsedValue
import com.intel.intelanalytics.domain.graphconstruction.OutputConfiguration
import com.intel.graphbuilder.parser.InputSchema
import com.intel.intelanalytics.domain.GraphTemplate
import com.intel.intelanalytics.domain.Schema
import com.intel.graphbuilder.parser.rule.ConstantValue
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig
import com.intel.intelanalytics.domain.graphconstruction.VertexRule
import com.intel.intelanalytics.domain.graphconstruction.Property
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils

class GraphBuilderConfigFactory(val schema: Schema, val graphTemplate: GraphTemplate) {

  def getInputSchema(schema: Schema): InputSchema = {
    val columns = new ListBuffer[ColumnDef]()

    for ((string, dataType) <- schema.columns) {
      val column = new ColumnDef(string, dataType.getClass)
      columns += column
    }

    new InputSchema(columns)
  }

  def getTitanConfiguration(graphName: String, outputConfiguration: OutputConfiguration): SerializableBaseConfiguration = {

    // Only use this method when the store is Titan
    require(outputConfiguration.storeName == "Titan")

    var titanConfiguration = new SerializableBaseConfiguration

    titanConfiguration.setProperty("storage.backend", "hbase")
    titanConfiguration.setProperty("storage.tablename", graphName + "_titan_hbase")

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

  def getGBValue(value: Value): GBValue = {
    if (value.source == GBValueSourcing.CONSTANT) {
      new ConstantValue(value.value)
    }
    else {
      new ParsedValue(value.value)
    }
  }

  def getGBPropertyRule(property: Property): GBPropertyRule = {
    new GBPropertyRule(getGBValue(property.key), getGBValue(property.value))
  }

  def getGBVertexRule(vertexRule: VertexRule): GBVertexRule = {
    new GBVertexRule(getGBPropertyRule(vertexRule.id), (vertexRule.properties map getGBPropertyRule))
  }

  def getGBVertexRules(vertexRules: List[VertexRule]): List[GBVertexRule] = {
    vertexRules map getGBVertexRule
  }

  def getGBEdgeRule(edgeRule: EdgeRule): GBEdgeRule = {
    new GBEdgeRule(getGBPropertyRule(edgeRule.tail), getGBPropertyRule(edgeRule.head),
      getGBValue(edgeRule.label), (edgeRule.properties map getGBPropertyRule))
  }

  def getGBEdgeRules(edgeRules: List[EdgeRule]): List[GBEdgeRule] = {
    edgeRules map getGBEdgeRule
  }

  val graphConfig: GraphBuilderConfig = {
    new GraphBuilderConfig(getInputSchema(schema),
      getGBVertexRules(graphTemplate.vertexRules),
      getGBEdgeRules(graphTemplate.edgeRules),
      getTitanConfiguration(graphTemplate.graphName, graphTemplate.outputConfig),
      biDirectional = graphTemplate.bidirectional,
      append = false,
      retainDanglingEdges = graphTemplate.retainDanglingEdges,
      inferSchema = true,
      broadcastVertexIds = false)
  }
}
