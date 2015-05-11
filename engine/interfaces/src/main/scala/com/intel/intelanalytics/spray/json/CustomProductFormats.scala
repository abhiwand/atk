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

package com.intel.intelanalytics.spray.json

import spray.json.{ StandardFormats, ProductFormats }
import org.apache.commons.lang3.StringUtils

/**
 * Override the default behavior in ProductFormats to convert case class property names
 * to lower_case_with_underscores names in JSON.
 */
trait CustomProductFormats extends ProductFormats {
  // StandardFormats is required by ProductFormats
  this: StandardFormats =>

  /**
   * Override the default behavior in ProductFormats to convert case class property names
   * to lower_case_with_underscores names in JSON.
   */
  override protected def extractFieldNames(classManifest: ClassManifest[_]): Array[String] = {
    super.extractFieldNames(classManifest)
      .map(name => JsonPropertyNameConverter.camelCaseToUnderscores(name))
  }

}
