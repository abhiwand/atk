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

package com.intel.intelanalytics.domain.graph

/**
 * Data needed to export a graph.
 * @param graph reference of the source graph
 * @param name name of the new copied target graph. optional. if not included a name should be generated.
 */
case class CopyGraphArgs(graph: GraphReference, name: Option[String] = None) {
  require(graph != null, "graph is required")
}
