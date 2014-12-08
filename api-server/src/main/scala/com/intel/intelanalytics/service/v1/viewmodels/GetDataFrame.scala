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

package com.intel.intelanalytics.service.v1.viewmodels

import com.intel.intelanalytics.domain.schema.Schema

/**
 * The REST service response for single dataFrame in "GET ../frames/id"
 *
 * @param id unique id auto-generated by the database
 * @param name name assigned by user, auto-assigned by system if user doesn't supply
 * @param schema the schema of the frame (defines columns, etc)
 * @param rowCount the number of rows in the frames
 * @param links
 */

case class GetDataFrame(id: Long, name: String,
                        ia_uri: String, schema: Schema,
                        rowCount: Option[Long],
                        links: List[RelLink],
                        errorFrameId: Option[Long],
                        commandPrefix: String) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name must not be null")
  require(ia_uri != null, "ia_uri must not be null")
  require(schema != null, "schema must not be null")
  require(rowCount.isEmpty || rowCount.get >= 0, "rowCount must not be negative")
  require(links != null, "links must not be null")
  require(errorFrameId != null, "errorFrameId must not be null")
  require(commandPrefix != null, "command prefix must not be null")
}