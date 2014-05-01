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

package com.intel.graphbuilder.write.titan

import com.intel.graphbuilder.schema.{ EdgeLabelDef, PropertyDef, PropertyType, GraphSchema }
import com.intel.graphbuilder.write.SchemaWriter
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints._

/**
 * Titan specific implementation of SchemaWriter.
 * <p>
 * This writer will ignore types that are already defined with the same name.
 * </p>
 */
class TitanSchemaWriter(graph: TitanGraph) extends SchemaWriter {

  if (graph == null) {
    throw new IllegalArgumentException("TitanSchemaWriter requires a non-null Graph")
  }
  if (!graph.isOpen) {
    throw new IllegalArgumentException("TitanSchemaWriter requires an open Graph")
  }

  /**
   * Write the schema definition to the underlying Titan graph database
   */
  override def write(schema: GraphSchema): Unit = {
    writePropertyDefs(schema.propertyDefs)
    writeLabelDefs(schema.edgeLabelDefs)
  }

  /**
   * Create a list of property types in Titan
   * @param propertyDefs the definition of a Property
   */
  private def writePropertyDefs(propertyDefs: List[PropertyDef]): Unit = {
    for (propertyDef <- propertyDefs) {
      writePropertyDef(propertyDef)
    }
  }

  /**
   * Create a property type in Titan
   * @param propertyDef the definition of a Property
   */
  private def writePropertyDef(propertyDef: PropertyDef): Unit = {
    if (graph.getType(propertyDef.name) == null) {
      val property = graph.makeKey(propertyDef.name).dataType(propertyDef.dataType)
      if (propertyDef.indexed) {
        // TODO: future: should we implement INDEX_NAME?
        property.indexed(indexType(propertyDef.propertyType))
      }
      if (propertyDef.unique) {
        property.unique()
      }
      property.make()
    }
  }

  /**
   * Determine the index type from the supplied PropertyType
   * @param propertyType enumeration to specify Vertex or Edge
   */
  private[titan] def indexType(propertyType: PropertyType.Value): Class[_ <: Element] = {
    if (propertyType == PropertyType.Vertex) {
      classOf[Vertex] // TODO: this should probably be an Index Type property?
    }
    else if (propertyType == PropertyType.Edge) {
      classOf[Edge] // TODO: this should probably be an Index Type property?
    }
    else {
      throw new RuntimeException("Unknown PropertyType is not yet implemented: " + propertyType)
    }
  }

  /**
   * Create the edge label definitions in Titan
   * @param edgeLabelDefs the edge labels needed in the schema
   */
  private def writeLabelDefs(edgeLabelDefs: List[EdgeLabelDef]): Unit = {
    // TODO: future: implement manyToOne(), sortKey(), etc.

    // TODO: do we want edges with certain labels to only support certain properties, seems like Titan supports this but it wasn't that way in GB2

    // TODO: implement signature, this was in GB2, not positive it is needed?
    //  ArrayList<TitanKey> titanKeys = new ArrayList<TitanKey>();
    //  signature()

    edgeLabelDefs.foreach(labelSchema =>
      if (graph.getType(labelSchema.label) == null) {
        graph.makeLabel(labelSchema.label).make()
      }
    )
  }
}
