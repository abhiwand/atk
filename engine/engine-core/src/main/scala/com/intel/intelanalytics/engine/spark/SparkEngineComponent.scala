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

package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.spark.gc.GarbageCollector
import com.intel.intelanalytics.engine.spark.command._
import com.intel.intelanalytics.engine.spark.util.{ EnvironmentLogger, JvmVersionReporter, DiskSpaceReporter }

/**
 * Main class for initializing the Spark Engine
 */
class SparkEngineComponent extends AbstractEngineComponent(new CommandLoader) {
  EnvironmentLogger.log()
  SparkEngineConfig.logSettings()
  JvmVersionReporter.check()
  DiskSpaceReporter.checkDiskSpace()
  metaStore.initializeSchema()
  GarbageCollector.startup(metaStore, frameFileStorage, backendGraphStorage)
}
