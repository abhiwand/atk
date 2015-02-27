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

package org.apache.spark.ia.graph

import com.intel.intelanalytics.domain.schema.{ VertexSchema, Schema }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Additional functions for RDD's of type Vertex
 * @param parent the RDD to wrap
 */
class VertexRDDFunctions(parent: RDD[Vertex]) {

  /**
   * Vertex frames from parent based on the labels provided
   * @param schemas by providing the list of schemas you can prevent an extra map/reduce to collect them
   * @return the VertexFrameRDD - one vertex type per RDD
   */
  def splitByLabel(schemas: List[VertexSchema]): List[VertexFrameRDD] = {
    parent.cache()

    val split = schemas.map(_.label).map(label => parent.filter(vertex => vertex.label == label))
    split.foreach(_.cache())

    val rowRdds = split.map(rdd => rdd.map(vertex => vertex.row))

    val results = schemas.zip(rowRdds).map { case (schema: Schema, rows: RDD[Row]) => new VertexFrameRDD(schema, rows) }

    parent.unpersist(blocking = false)
    split.foreach(_.unpersist(blocking = false))
    results
  }

  /**
   * Split parent (mixed vertex types) into a list of vertex frames (one vertex type per frame)
   *
   * IMPORTANT! does not perform as well as splitByLabel(schemas) - extra map() and distinct()
   */
  def splitByLabel(): List[VertexFrameRDD] = {
    parent.cache()
    val schemas = parent.map(vertex => vertex.schema.asInstanceOf[VertexSchema]).distinct().collect().toList
    splitByLabel(schemas)
  }

}
