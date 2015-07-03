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

import com.intel.taproot.analytics.domain.frame.FrameReference
import com.intel.taproot.analytics.domain.model.ModelReference
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }

/**
 * Command for predicting labels on the given dataset using a libsvm model
 */
case class LibSvmPredictArgs(@ArgDoc("""Handle to the model to be written to.""") model: ModelReference,
                             @ArgDoc("""A frame whose labels are to be predicted.""") frame: FrameReference,
                             @ArgDoc("""Column(s) containing the observations whose labels are to be
                                predicted.
                                Default is the columns the LibsvmModel was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")

}
