/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, GraphSchema }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRdd, SparkFrameStorage }
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object FilterVerticesFunctions {

  /**
   * Remove dangling edges after filtering on the vertices
   * @param vertexLabel vertex label of the filtered vertex frame
   * @param frameStorage frame storage
   * @param seamlessGraph seamless graph instance
   * @param sc spark context
   * @param filteredRdd rdd with predicate applied
   */
  def removeDanglingEdges(vertexLabel: String, frameStorage: SparkFrameStorage, seamlessGraph: SeamlessGraphMeta,
                          sc: SparkContext, filteredRdd: FrameRdd)(implicit invocation: Invocation): Unit = {
    val vertexFrame = seamlessGraph.vertexMeta(vertexLabel)
    val vertexFrameSchema = vertexFrame.schema

    val originalVertexIds = frameStorage.loadFrameData(sc, vertexFrame).mapRows(_.value(GraphSchema.vidProperty))
    val filteredVertexIds = filteredRdd.mapRows(_.value(GraphSchema.vidProperty))
    val droppedVertexIdsRdd = originalVertexIds.subtract(filteredVertexIds)

    // drop edges connected to the vertices
    seamlessGraph.edgeFrames.map(frame => {
      val edgeSchema = frame.schema.asInstanceOf[EdgeSchema]
      if (edgeSchema.srcVertexLabel.equals(vertexLabel)) {
        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, sc, droppedVertexIdsRdd, frame, GraphSchema.srcVidProperty)
      }
      else if (edgeSchema.destVertexLabel.equals(vertexLabel)) {
        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, sc, droppedVertexIdsRdd, frame, GraphSchema.destVidProperty)
      }
    })
  }

  /**
   * Remove dangling edges in edge frame and save.
   * @param frameStorage frame storage
   * @param sc spark context
   * @param droppedVertexIdsRdd rdd of vertex ids
   * @param edgeFrame edge frame
   * @param vertexIdColumn source vertex id column or destination id column
   * @return dataframe object
   */
  def dropDanglingEdgesAndSave(frameStorage: SparkFrameStorage,
                               sc: SparkContext,
                               droppedVertexIdsRdd: RDD[Any],
                               edgeFrame: FrameEntity,
                               vertexIdColumn: String)(implicit invocation: Invocation): FrameEntity = {
    val edgeRdd = frameStorage.loadLegacyFrameRdd(sc, edgeFrame)
    val remainingEdges = FilterVerticesFunctions.dropDanglingEdgesFromEdgeRdd(edgeRdd,
      edgeFrame.schema.columnIndex(vertexIdColumn), droppedVertexIdsRdd)
    val frameRdd = new LegacyFrameRdd(edgeFrame.schema, remainingEdges).toFrameRdd()
    frameStorage.saveFrameData(edgeFrame.toReference, frameRdd)
  }

  /**
   * Remove dangling edges in rdd
   * @param edgeRdd edge rdd
   * @param vertexIdColumnIndex column index of vertex id in edge row
   * @param droppedVertexIdsRdd rdd of vertex ids
   * @return a edge rdd with dangling edges removed
   */
  def dropDanglingEdgesFromEdgeRdd(edgeRdd: LegacyFrameRdd, vertexIdColumnIndex: Int, droppedVertexIdsRdd: RDD[Any]): RDD[Row] = {
    val keyValueEdgeRdd = edgeRdd.map(row => (row(vertexIdColumnIndex), row))
    val droppedVerticesPairRdd = droppedVertexIdsRdd.map(vid => (vid, null))
    keyValueEdgeRdd.leftOuterJoin(droppedVerticesPairRdd).filter {
      case (key, (leftResult, rightResult)) => rightResult match {
        case None => true
        case _ => false
      }
    }.map { case (key, (leftResult, rightResult)) => leftResult }
  }
}
