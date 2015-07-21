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

package com.intel.taproot.analytics.engine.graph

import com.intel.taproot.analytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.taproot.analytics.domain.graph.SeamlessGraphMeta
import com.intel.taproot.analytics.domain.schema.VertexSchema
import com.intel.taproot.analytics.engine.plugin.Invocation
import com.intel.taproot.analytics.engine.{ GraphStorage, FrameStorage }
import com.intel.taproot.analytics.engine.frame._
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.ia.graph.VertexFrameRdd

trait VertexFrame extends Frame {

  override def schema: VertexSchema

  def graph: SeamlessGraphMeta
}

object VertexFrame {

  implicit def vertexFrameToFrameReference(vertexFrame: VertexFrame): FrameReference = vertexFrame.entity.toReference

  implicit def vertexFrameToFrameEntity(vertexFrame: VertexFrame): FrameEntity = vertexFrame.entity
}

trait SparkVertexFrame extends VertexFrame {

  /** Load the frame's data as an RDD */
  def rdd: VertexFrameRdd

  /** Update the data for this frame */
  def save(rdd: VertexFrameRdd): SparkVertexFrame

}

class VertexFrameImpl(frame: FrameReference, frameStorage: FrameStorage, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends FrameImpl(frame, frameStorage)(invocation)
    with VertexFrame {

  override def entity: FrameEntity = {
    val e = super.entity
    require(e.isVertexFrame, s"VertexFrame is required but found other frame type: $e")
    e
  }

  override def schema: VertexSchema = super.schema.asInstanceOf[VertexSchema]

  override def graph: SeamlessGraphMeta = {
    sparkGraphStorage.expectSeamless(entity.graphId.getOrElse(throw new RuntimeException("VertxFrame is required to have a graphId but this one didn't")))
  }

}

class SparkVertexFrameImpl(frame: FrameReference, sc: SparkContext, sparkFrameStorage: SparkFrameStorage, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends VertexFrameImpl(frame, sparkFrameStorage, sparkGraphStorage)(invocation)
    with SparkVertexFrame {

  /** Load the frame's data as an RDD */
  override def rdd: VertexFrameRdd = sparkGraphStorage.loadVertexRDD(sc, frame)

  /** Update the data for this frame */
  override def save(rdd: VertexFrameRdd): SparkVertexFrame = {
    val result = sparkGraphStorage.saveVertexRdd(frame, rdd)
    new SparkVertexFrameImpl(result, sc, sparkFrameStorage, sparkGraphStorage)
  }

}