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

package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements.{ Property, GbIdToPhysicalId, GBVertex }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.StringUtils
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Functions that are applicable to Vertex RDD's.
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */
class VertexRddFunctions(self: RDD[GBVertex]) {

  /**
   * Merge duplicate Vertices, creating a new Vertex that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[GBVertex] = {
    self.groupBy(m => m.id).mapValues(dups => dups.reduce((m1, m2) => m1.merge(m2))).values
  }

  /**
   * Convert "Vertices with or without _label property" into "Vertices with _label property"
   * @param indexNames Vertex properties that have been indexed (fallback for labels)
   * @return Vertices with _label property
   */
  def labelVertices(indexNames: List[String]): RDD[GBVertex] = {
    self.map(vertex => {
      val columnNames = vertex.fullProperties.map(_.key)
      val indexedProperties = indexNames.intersect(columnNames.toSeq)
      val userDefinedColumn = if (indexedProperties.isEmpty) None else Some(indexedProperties.head)

      val label = if (vertex.getProperty("_label").isDefined && vertex.getProperty("_label").get.value != null) {
        vertex.getProperty("_label").get.value
      }
      else if (userDefinedColumn.isDefined) {
        userDefinedColumn.get
      }
      else {
        "unlabeled"
      }
      val props = vertex.properties.filter(_.key != "_label") + Property("_label", label)
      new GBVertex(vertex.physicalId, vertex.gbId, props)
    })
  }

  /**
   * Write to Titan and produce a mapping of GbId's to Physical Id's
   * <p>
   * This is an unusual transformation because it has the side effect of writing to Titan.
   * This means extra care is needed to prevent it from being recomputed.
   * </p>
   * @param append true to append to an existing graph
   */
  def write(titanConnector: TitanGraphConnector, append: Boolean): RDD[GbIdToPhysicalId] =
    new TitanVertexWriterRdd(self, titanConnector, append)

}
