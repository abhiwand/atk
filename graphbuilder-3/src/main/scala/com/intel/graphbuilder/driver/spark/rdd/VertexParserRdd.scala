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

import com.intel.graphbuilder.elements.{ Property, GBVertex }
import com.intel.graphbuilder.parser.Parser
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, TaskContext }

import scala.collection.mutable.Map

/**
 * Parse the raw rows of input into Vertices
 *
 * @param vertexParser the parser to use
 */
class VertexParserRdd(prev: RDD[Seq[_]], vertexParser: Parser[GBVertex]) extends RDD[GBVertex](prev) {

  override def getPartitions: Array[Partition] = firstParent[GBVertex].partitions

  /**
   * Parse the raw rows of input into Vertices
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GBVertex] = {

    // In some data sets many vertices are duplicates and many of the duplicates are
    // 'near' each other in the parsing process (like Netflix movie data where the
    // rows define both edges and vertices and the rows are ordered by user id). By
    // keeping a map and merging the duplicates that occur in a given split, there
    // will be less to deal with later. This is like a combiner in Hadoop Map/Reduce,
    // it won't remove all duplicates in the final RDD but there will be less to
    // shuffle later.  For input without duplicates, this shouldn't add much overhead.
    val vertexMap = Map[Property, GBVertex]()

    firstParent[Seq[_]].iterator(split, context).foreach(row => {
      vertexParser.parse(row).foreach(v => {
        val opt = vertexMap.get(v.gbId)
        if (opt.isDefined) {
          vertexMap.put(v.gbId, v.merge(opt.get))
        }
        else {
          vertexMap.put(v.gbId, v)
        }
      })
    })

    vertexMap.valuesIterator
  }
}
