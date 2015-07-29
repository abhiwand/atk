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

package com.intel.taproot.atk.graph.plugins.graphclustering

import com.intel.taproot.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.taproot.graphbuilder.util.SerializableBaseConfiguration

case class GraphClusteringStorageFactory(dbConnectionConfig: SerializableBaseConfiguration)
    extends GraphClusteringStorageFactoryInterface {

  override def newStorage(): GraphClusteringStorage = {
    val titanConnector = new TitanGraphConnector(dbConnectionConfig)
    val titanGraph = titanConnector.connect()
    new GraphClusteringStorage(titanGraph)
  }
}