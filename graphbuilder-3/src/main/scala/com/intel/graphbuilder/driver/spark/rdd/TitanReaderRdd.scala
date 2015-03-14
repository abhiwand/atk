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

import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.TitanConverter
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{ InterruptibleIterator, Partition, TaskContext }

/**
 * RDD that loads Titan graph from HBase.
 *
 * @param faunusRdd Input RDD
 * @param titanConnector connector to Titan
 */

class TitanReaderRdd(faunusRdd: RDD[(NullWritable, FaunusVertex)], titanConnector: TitanGraphConnector) extends RDD[GraphElement](faunusRdd) {

  override def getPartitions: Array[Partition] = firstParent[(NullWritable, FaunusVertex)].partitions

  /**
   * Parses HBase input rows to extract vertices and corresponding edges.
   *
   * @return Iterator of GraphBuilder vertices and edges using GraphBuilder's GraphElement trait
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GraphElement] = {

    val graphElements = firstParent[(NullWritable, FaunusVertex)].iterator(split, context).flatMap(inputRow => {
      val faunusVertex = inputRow._2

      val gbVertex = TitanConverter.createGraphBuilderVertex(faunusVertex)
      val gbEdges = TitanConverter.createGraphBuilderEdges(faunusVertex)

      val rowGraphElements: Iterator[GraphElement] = Iterator(gbVertex) ++ gbEdges

      rowGraphElements
    })

    new InterruptibleIterator(context, graphElements)
  }

}
