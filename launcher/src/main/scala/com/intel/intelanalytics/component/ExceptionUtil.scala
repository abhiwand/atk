package com.intel.intelanalytics.component

import scala.util.control.NonFatal

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
object ExceptionUtil {
  /**
   * Run the given expression. If an exception is thrown, instead throw an Exception
   * that wraps the original, and uses the given failure message.
   * @param expr the expression to evaluate
   * @param failureMessage the failure message to use if an exception is thrown
   * @tparam T the return type of the expression
   * @return the result of evaluating the expression, or else throws Exception.
   */
  def attempt[T](expr: => T, failureMessage: => String) = {
    try {
      expr
    }
    catch {
      case NonFatal(e) => throw new Exception(failureMessage, e)
    }
  }
}
