//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.domain.gc

import com.intel.intelanalytics.domain.HasId
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

  def isRunning(): Boolean = endTime != None
}