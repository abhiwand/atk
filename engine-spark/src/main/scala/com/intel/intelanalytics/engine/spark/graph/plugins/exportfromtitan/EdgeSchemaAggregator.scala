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

package com.intel.intelanalytics.engine.spark.graph.plugins.exportfromtitan

import com.intel.graphbuilder.elements.GBEdge
import com.intel.intelanalytics.domain.schema._

/**
 * Aggregation methods for getting EdgeSchema from GBEdges
 */
object EdgeSchemaAggregator extends Serializable {

  def toSchema(edgeHolder: EdgeHolder): EdgeSchema = {
    val columns = edgeHolder.edge.properties.map(property => Column(property.key, DataTypes.dataTypeOfValue(property.value)))
    val columnNames = edgeHolder.edge.properties.map(_.key)
    val allColumns = GraphSchema.edgeSystemColumns ++ columns.filterNot(col => GraphSchema.edgeSystemColumnNamesSet.contains(col.name))

    new EdgeSchema(allColumns.toList, edgeHolder.edge.label, edgeHolder.srcLabel, edgeHolder.destLabel, directed = true)
  }

  val zeroValue = Map[String, EdgeSchema]()

  def seqOp(edgeSchemas: Map[String, EdgeSchema], edgeHolder: EdgeHolder): Map[String, EdgeSchema] = {
    val schema = toSchema(edgeHolder)
    val label = schema.label
    if (edgeSchemas.contains(label)) {
      val schemaCombined = edgeSchemas.get(label).get.union(schema).asInstanceOf[EdgeSchema]
      edgeSchemas + (label -> schemaCombined)
    }
    else {
      edgeSchemas + (label -> schema)
    }
  }

  def combOp(edgeSchemasA: Map[String, EdgeSchema], edgeSchemasB: Map[String, EdgeSchema]): Map[String, EdgeSchema] = {
    val combinedKeys = edgeSchemasA.keySet ++ edgeSchemasB.keySet
    val combined = combinedKeys.map(key => {
      val valueA = edgeSchemasA.get(key)
      val valueB = edgeSchemasB.get(key)
      if (valueA.isDefined) {
        if (valueB.isDefined) {
          key -> valueA.get.union(valueB.get).asInstanceOf[EdgeSchema]
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
