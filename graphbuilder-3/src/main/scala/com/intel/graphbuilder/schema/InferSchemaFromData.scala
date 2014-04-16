package com.intel.graphbuilder.schema

import com.intel.graphbuilder.elements.{Property, Vertex, Edge}
import scala.collection.mutable.Map

/**
 * Infer the schema from the GraphElements themselves after parsing.
 *
 * This is more overhead but allows cases like dynamic labels, i.e. labels that are parsed from the input.
 */
class InferSchemaFromData extends Serializable {

  var edgeLabelDefsMap = Map[String, EdgeLabelDef]()
  var propertyDefsMap = Map[String, PropertyDef]()

  /**
   * Add an Edge to the inferred schema.
   */
  def add(edge: Edge): Unit = {
    addEdgeLabel(edge)
    addProperties(PropertyType.Edge, edge.properties)
  }

  /**
   * Add a label to the map, if it isn't already there.
   */
  def addEdgeLabel(edge: Edge): Unit = {
    if (edgeLabelDefsMap.get(edge.label).isEmpty) {
      edgeLabelDefsMap += (edge.label -> new EdgeLabelDef(edge.label))
    }
  }

  /**
   * Add a Vertex to the inferred schema.
   */
  def add(vertex: Vertex): Unit = {
    addProperty(PropertyType.Vertex, vertex.gbId, isGbId = true)
    addProperties(PropertyType.Vertex, vertex.properties)
  }

  /**
   * Get the inferred GraphSchema.  Do this after adding Edges and Vertices.
   */
  def graphSchema: GraphSchema = {
    new GraphSchema(edgeLabelDefsMap.values.toList, propertyDefsMap.values.toList)
  }

  /**
   * Add a list of properties, if they aren't already present.
   */
  private def addProperties(propertyType: PropertyType.Value, properties: Seq[Property]): Unit = {
    properties.foreach(prop => addProperty(propertyType, prop, isGbId = false))

  }

  /**
   * Add a property, if it isn't already present.
   * @param propertyType Vertex or Edge
   * @param property property to add
   * @param isGbId true if this property is a Vertex gbId
   */
  private def addProperty(propertyType: PropertyType.Value, property: Property, isGbId: Boolean): Unit = {
    if (propertyDefsMap.get(property.key).isEmpty) {
      propertyDefsMap += (property.key -> new PropertyDef(propertyType, property.key, property.value.getClass, isGbId, isGbId))
    }
  }

}