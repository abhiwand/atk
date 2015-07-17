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

package com.intel.taproot.analytics.engine.spark.gc
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments used for a single execution of garbage collection
 * @param ageToDeleteData minimum age of entity for data deletion. defaults to server config
 * @param ageToDeleteMetaData minimum age of entity for meta data deletion. defaults to server config
 */
case class GarbageCollectionArgs(
  @ArgDoc("""Minimum age of entity for data deletion.
Defaults to server config.""") ageToDeleteData: Option[String] = None,
  @ArgDoc("""Minimum age of entity for meta data deletion.
Defaults to server config.""") ageToDeleteMetaData: Option[String] = None)
