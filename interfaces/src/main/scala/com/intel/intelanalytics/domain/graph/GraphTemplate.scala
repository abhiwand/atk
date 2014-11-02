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

package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.StorageFormats

/**
 * Arguments for creating the metadata entry for a graph.
 * @param name The user's name for the graph.
 */
case class GraphTemplate(name: String, storageFormat: String = StorageFormats.SeamlessGraph) {
  require(name != null, "name must not be null")
  require(name.trim.length > 0, "name must not be empty or whitespace")

  require(storageFormat != null, "storageFormat must not be null")
  require(storageFormat.trim.length > 0, "storageFormat must not be empty or whitespace")
  StorageFormats.validateGraphFormat(storageFormat)
}
