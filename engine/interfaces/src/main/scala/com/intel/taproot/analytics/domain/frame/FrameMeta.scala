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

package com.intel.taproot.analytics.domain.frame

import com.intel.taproot.analytics.domain.Naming

case class FrameName(name: String)

object FrameName extends Naming("frame")

import com.intel.taproot.analytics.domain.HasMetaData

/**
 * A FrameReference with metadata
 */
@deprecated("this was a partially developed concept that we want to get rid of")
class FrameMeta(frame: FrameEntity) extends FrameReference(frame.id) with HasMetaData {

  type Meta = FrameEntity

  @deprecated("this was a partially developed concept that we want to get rid of")
  val meta = frame

}
