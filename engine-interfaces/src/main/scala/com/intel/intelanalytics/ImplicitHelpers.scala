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

package com.intel.intelanalytics

/**
 * http://stackoverflow.com/questions/4403906/is-it-possible-in-scala-to-force-the-caller-to-specify-a-type-parameter-for-a-po
 */
sealed class DefaultsTo[A, B]

trait LowPriorityDefaultsTo {
  implicit def overrideDefault[A, B] = new DefaultsTo[A, B]
}

object DefaultsTo extends LowPriorityDefaultsTo {
  implicit def default[B] = new DefaultsTo[B, B]
}

/**
 * http://stackoverflow.com/a/4580176
 */
sealed trait NotNothing[T] { type U }
object NotNothing {
  implicit val nothingIsNothing = new NotNothing[Nothing] { type U = Any }
  implicit def notNothing[T] = new NotNothing[T] { type U = T }
}
