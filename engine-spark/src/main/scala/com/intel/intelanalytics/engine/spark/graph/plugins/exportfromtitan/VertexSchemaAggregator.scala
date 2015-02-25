//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.graph.plugins.exportfromtitan

import com.intel.graphbuilder.elements.GBVertex
import com.intel.intelanalytics.domain.schema
import com.intel.intelanalytics.domain.schema._

import scala.collection.immutable.Map

/**
 * Aggregation methods for getting VertexSchema from GBVertices
 *
 * @param indexNames vertex properties with unique indexes in Titan
 */
class VertexSchemaAggregator(indexNames: List[String]) extends Serializable {

  def toSchema(vertex: GBVertex): VertexSchema = {
    val columns = vertex.properties.map(property => Column(property.key, DataTypes.dataTypeOfValue(property.value)))

    val columnNames = vertex.properties.map(_.key)
    val indexedProperties = indexNames.intersect(columnNames.toSeq)
    val userDefinedColumn = if (indexedProperties.isEmpty) Some("titanPhysicalId") else Some(indexedProperties.head)
    val label = vertex.getProperty("_label").get.value.toString

    val allColumns = schema.GraphSchema.vertexSystemColumns ++ columns.filterNot(col => GraphSchema.vertexSystemColumnNamesSet.contains(col.name))

    new VertexSchema(allColumns.toList, label, userDefinedColumn)
  }

  val zeroValue = Map[String, VertexSchema]()

  def seqOp(vertexSchemas: Map[String, VertexSchema], vertex: GBVertex): Map[String, VertexSchema] = {
    val schema = toSchema(vertex)
    val label = schema.label
    if (vertexSchemas.contains(label)) {
      val schemaCombined = vertexSchemas.get(label).get.union(schema).asInstanceOf[VertexSchema]
      vertexSchemas + (label -> schemaCombined)
    }
    else {
      vertexSchemas + (label -> schema)
    }
  }

  def combOp(vertexSchemasA: Map[String, VertexSchema], vertexSchemasB: Map[String, VertexSchema]): Map[String, VertexSchema] = {
    val combinedKeys = vertexSchemasA.keySet ++ vertexSchemasB.keySet
    val combined = combinedKeys.map(key => {
      val valueA = vertexSchemasA.get(key)
      val valueB = vertexSchemasB.get(key)
      if (valueA.isDefined) {
        if (valueB.isDefined) {
          key -> valueA.get.union(valueB.get).asInstanceOf[VertexSchema]
        }
        else {
          key -> valueA.get
        }
      }
      else {
        key -> valueB.get
      }
    })
    combined.toMap
  }

}
