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

package com.intel.taproot.analytics.libSvmPlugins

import com.intel.taproot.analytics.domain.model.ModelReference

import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for loading model data into existing model in the model database.
 */
case class LibSvmScoreArgs(
    @ArgDoc("""Handle to the model to be written to.""") model: ModelReference,
    @ArgDoc("""A single observation of features.""") vector: Vector[Double]) {
  require(model != null, "model is required")
}

case class LibSvmScoreReturn(prediction: Double)
