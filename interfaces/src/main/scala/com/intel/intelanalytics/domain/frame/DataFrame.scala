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

package com.intel.intelanalytics.domain.frame

import com.intel.intelanalytics.domain.HasId
import com.intel.intelanalytics.domain.schema.Schema
import org.joda.time.DateTime

/**
 * Represents a Big Data Frame
 * @param id unique id auto-generated by the database
 * @param name name assigned by user, auto-assigned by system if user doesn't supply
 * @param description description of frame (a good default description might be the name of the input file)
 * @param uri absolute path to the file backing the frame in HDFS
 * @param schema the schema of the frame (defines columns, etc)
 * @param status lifecycle status. For example, INIT (building), ACTIVE, DELETED (un-delete possible), DELETE_FINAL (no un-delete), INCOMPLETE (failed construction)
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 * @param createdBy user who created this row
 * @param modifiedBy user who last modified this row
 */
case class DataFrame(id: Long,
                     name: String,
                     description: Option[String] = None,
                     uri: String,
                     schema: Schema = Schema(),
                     status: Long,
                     createdOn: DateTime,
                     modifiedOn: DateTime,
                     createdBy: Option[Long],
                     modifiedBy: Option[Long]) extends HasId {
  require(id >= 0)
  require(name != null)
  require(name.trim.length > 0)
}