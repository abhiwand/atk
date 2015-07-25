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

package com.intel.taproot.analytics.scoring.models

import com.intel.taproot.analytics.scoring.interfaces.{ Model, ModelLoader }

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
/**
 * Base interface for a Model loader.
 */
class TestLibKMeansModel() extends Object with Model {

  override def score(data: Seq[Array[String]]): Future[Seq[Any]] = future {
    var score = Seq[Any]()
    println("model is scoring")
    score
  }

}