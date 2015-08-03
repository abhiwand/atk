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

package org.trustedanalytics.atk.domain.model

import org.trustedanalytics.atk.domain.UriReference

import com.intel.taproot.analytics.engine.plugin.{ Invocation }

/**
 * ModelReference is the model's unique identifier. It is used to generate the ia_uri for the model.
 */
case class ModelReference(modelId: Long) extends UriReference {
  /** The entity id */
  override def id: Long = modelId

  /** The entity name e.g. "frame", "graph", ... */
  override def name: String = "model"
}

