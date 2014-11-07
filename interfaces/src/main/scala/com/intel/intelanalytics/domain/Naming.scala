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
package com.intel.intelanalytics.domain

/**
 * General user object naming
 */
object Naming {
  private lazy val alphaNumericUnderscorePattern = "^[a-zA-Z0-9_]+$".r

  /**
   * Determines whether the given text contains exclusively alphanumeric and underscore chars
   */
  def isAlphaNumericUnderscore(text: String): Boolean = (alphaNumericUnderscorePattern findFirstIn text).nonEmpty

  /**
   * Raises an exception if the given text contains anything but alphanumeric or underscore chars
   * @param text subject
   * @return subject
   */
  def validateAlphaNumericUnderscore(text: String): String = {
    if (!isAlphaNumericUnderscore(text)) {
      throw new IllegalArgumentException(s"Invalid string '$text', only alphanumeric and underscore permitted")
    }
    text
  }

  /**
   * Raises an exception if the given text Option contains anything but alphanumeric or underscore chars
   * If the Option is none, an empty string is returned
   * @param text subject
   * @return subject or empty string
   */
  def validateAlphaNumericUnderscoreOrNone(text: Option[String]): String = {
    text match {
      case Some(name) => validateAlphaNumericUnderscore(name)
      case None => ""
    }
  }

  /**
   * Raises an exception if the given text Option contains anything but alphanumeric or underscore chars
   * If the Option is none, an empty string is returned
   * @param text subject
   * @return subject or empty string
   */
  def validateAlphaNumericUnderscoreOrGenerate(text: Option[String], generate: => String): String = {
    text match {
      case Some(name) => validateAlphaNumericUnderscore(name)
      case None => generate
    }
  }

  /**
   * Generates a unique name, w/ optional prefix and suffix
   *
   * @param prefix Optional annotation prefix  (must be alphanumeric or underscore)
   * @param suffix Optional annotation suffix  (must be alphanumeric or underscore)
   * @return generated name
   */
  def generateName(prefix: Option[String] = None, suffix: Option[String] = None): String = {
    val p = validateAlphaNumericUnderscoreOrNone(prefix)
    val s = validateAlphaNumericUnderscoreOrNone(suffix)
    p + java.util.UUID.randomUUID().toString.filterNot(c => c == '-') + s
  }
}
