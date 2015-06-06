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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.query.{ QueryTemplate, Query }
import com.intel.intelanalytics.engine.plugin.Invocation
import spray.json.JsObject
import scala.util.Try

/**
 * Interface describing the management of Query database storage
 */
trait QueryStorage {
  def lookup(id: Long)(implicit invocation: Invocation): Option[Query]
  def create(frame: QueryTemplate)(implicit invocation: Invocation): Query
  def scan(offset: Int, count: Int)(implicit invocation: Invocation): Seq[Query]
  def start(id: Long)(implicit invocation: Invocation): Unit
  def complete(id: Long, result: Try[JsObject])(implicit invocation: Invocation): Unit
}
