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

import spray.json.JsonFormat

/**
 * Generic String value that can be used by plugins that return a single String
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class StringValue(value: String)

/**
 * Generic Long value that can be used by plugins that return a single Long
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class LongValue(value: Long)

/**
 * Generic Int value that can be used by plugins that return a single Int
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class IntValue(value: Int)

/**
 * Generic Double value that can be used by plugins that return a single Double
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class DoubleValue(value: Double)

/**
 * Generic boolean value that can be used by plugins that return a Boolean
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class BoolValue(value: Boolean)

/**
 * Generic singleton or list value which is a List, but has a Json serializer such that a singleton
 * is accepted
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class SingletonOrListValue[T](value: List[T])

/**
 * Generic double value that can be used by plugins that return a Double
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class VectorValue(value: Vector[Double])
