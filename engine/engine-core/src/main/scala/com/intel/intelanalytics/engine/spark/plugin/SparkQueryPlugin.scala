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

package com.intel.intelanalytics.engine.spark.plugin

import com.intel.intelanalytics.engine.plugin.{ QueryPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.SparkEngine
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

///**
// * Base trait for query plugins that need direct access to a SparkContext
// *
// * @tparam Argument the argument type for the query
// */
//trait SparkQueryPlugin[Argument <: Product]
//  extends QueryPlugin[Argument] with SparkPlugin[Argument]
