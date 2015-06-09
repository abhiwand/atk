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

package com.intel.intelanalytics

/**
 * http://stackoverflow.com/questions/4403906/is-it-possible-in-scala-to-force-the-caller-to-specify-a-type-parameter-for-a-po
 */
sealed class DefaultsTo[A, B]

trait LowPriorityDefaultsTo {
  implicit def overrideDefault[A, B] = new DefaultsTo[A, B]
}

object DefaultsTo extends LowPriorityDefaultsTo {
  implicit def default[B] = new DefaultsTo[B, B]
}

/**
 * http://stackoverflow.com/a/4580176
 */
sealed trait NotNothing[T] { type U }
object NotNothing {
  implicit val nothingIsNothing = new NotNothing[Nothing] { type U = Any }
  implicit def notNothing[T] = new NotNothing[T] { type U = T }
}
