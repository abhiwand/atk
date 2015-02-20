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

/**
 * Arguments needed to create a new entity
 *
 * @param name - e.g. for the entity type "model:logistic_regression", the subtype is "logistic_regression"
 * @param entityType - e.g. for the entity type "model:logistic_regression", the subtype is "logistic_regression"
 * @param description - optional description of the particular entity or why it was created
 */
case class CreateEntityArgs(name: Option[String] = None, entityType: Option[String] = None, description: Option[String] = None) {
  require(name != null)
  require(entityType != null)
  require(description != null)
  Naming.validateAlphaNumericUnderscoreOrNone(name)
}
