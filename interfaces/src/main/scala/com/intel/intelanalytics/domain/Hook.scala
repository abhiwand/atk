//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.domain

import org.joda.time.DateTime

// TODO: this class is not yet used but there will be a table for storing Hooks like this in the DB

/**
 * Hooks are registered callbacks.
 *
 * A hook allows a client to ask to be notified when a Command is completed.
 *
 * @param id unique id auto-generated by the database for this hook
 * @param commandId the uniqueId for a Command that this hook is a callback for
 * @param uri the URI to POST to when the Command has completed
 * @param completed true if this hook has completed (either successful or done retrying in case of failure)
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
@deprecated("hooks were planned but then we still haven't implemented after several months", "12-03-2014")
case class Hook(id: Long, commandId: Long, uri: String, completed: Boolean = false, createdOn: DateTime, modifiedOn: DateTime) extends HasId {

}
