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

package com.intel.taproot.spark.graphon.graphclustering

import java.io.Serializable

import com.intel.taproot.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.taproot.graphbuilder.schema.{ EdgeLabelDef, GraphSchema, PropertyDef, PropertyType }
import com.intel.taproot.graphbuilder.util.SerializableBaseConfiguration
import com.intel.taproot.graphbuilder.write.titan.TitanSchemaWriter
import com.intel.taproot.analytics.domain.schema.GraphSchema
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

trait GraphClusteringStorageInterface extends Serializable {

  def addSchema(): Unit

  def addVertexAndEdges(src: Long, dest: Long, metaNodeCount: Long, metaNodeName: String, iteration: Int): Long

  def commit(): Unit

  def shutdown(): Unit
}