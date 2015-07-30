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

package com.intel.taproot.analytics.domain.gc

import com.intel.taproot.analytics.domain.HasId
import org.joda.time.DateTime

/**
 * The metadata for the execution of the garbage collection
 * @param id unique id auto-generated by the database
 * @param hostname Hostname of the machine that executed the garbage collection
 * @param processId Process ID of the process that executed the garbage collection
 * @param startTime Start Time of the Garbage Collection execution
 * @param endTime End Time of the Garbage Collection execution
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class GarbageCollection(id: Long,
                             hostname: String,
                             processId: Long,
                             startTime: DateTime,
                             endTime: Option[DateTime],
                             createdOn: DateTime,
                             modifiedOn: DateTime) extends HasId {
  require(id >= 0, "id must be zero or greater")
  require(hostname != null && hostname.trim.length > 0, "HostName must not be null, empty, or whitespace")
  require(processId > 0, "ProcessID must be greater than 0")
  require(startTime != null, "StartTime must not be null")
}
