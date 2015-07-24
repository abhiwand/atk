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

package com.intel.taproot.analytics.engine.frame.plugins.join

import com.intel.taproot.analytics.domain.frame.FrameReference

import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for Join plugin
 *
 */
case class JoinArgs(@ArgDoc("""Join arguments for first data frame.""") leftFrame: JoinFrameArgs,
                    @ArgDoc("""Join arguments for first data frame.""") rightFrame: JoinFrameArgs,
                    @ArgDoc("""Methods of join (inner, left, right or outer).""") how: String,
                    @ArgDoc("""Name of new frame to be created.""") name: Option[String] = None) {
  require(leftFrame != null && leftFrame.frame != null, "left frame is required")
  require(rightFrame != null && rightFrame.frame != null, "right frame is required")
  require(leftFrame.joinColumn != null, "left join column is required")
  require(rightFrame.joinColumn != null, "right join column is required")
  require(how != null, "join method is required")
}

/**
 * Join arguments for frame
 *
 * @param frame Data frame
 * @param joinColumn
 */
case class JoinFrameArgs(frame: FrameReference, joinColumn: String)
