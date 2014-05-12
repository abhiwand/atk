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

class GraphBuilderConfigFactory(val schema: Schema, val graphTemplate: GraphTemplate) {

  def getInputSchema(schema: Schema): InputSchema = {
    val columns = new ListBuffer[ColumnDef]()

    for ((string, dataType) <- schema.columns) {
      val column = new ColumnDef(string, dataType.getClass)
      columns += column
    }

    new InputSchema(columns)
  }

  def getTitanConfiguration(outputConfiguration: OutputConfiguration): SerializableBaseConfiguration = {

    var titanConfiguration = new SerializableBaseConfiguration

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
      getGBEdgeRules(graphTemplate.edgeRules), getTitanConfiguration(graphTemplate.outputConfig),
      biDirectional = graphTemplate.bidirectional,
      append = false,
      retainDanglingEdges = graphTemplate.retainDanglingEdges,
      inferSchema = true,
      broadcastVertexIds = false)
  }
}
