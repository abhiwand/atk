/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.domain.gc

import org.joda.time.DateTime

/**
 * Arguments needed for creating a new instance of a GarbageCollection
 * @param hostname Hostname of the machine that executed the garbage collection
 * @param processId Process ID of the process that executed the garbage collection
 * @param startTime Start Time of the Garbage Collection execution
 */
case class GarbageCollectionTemplate(hostname: String,
                                     processId: Long,
                                     startTime: DateTime)
