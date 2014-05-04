//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
 * An invocation of a function defined on the server.
 * @param name the name of the command to be performed. In the case of a builtin command, this name is used to
 *             find the stored implementation. For a user command, this name is purely for descriptive purposes.
 * @param arguments the arguments to the function. In some cases, such as line parsers, the arguments are configuration
 *                  arguments that configure the parser before any input arrives. In other cases, such as training an
 *                  ML algorithm, the parameters are used to execute the function directly.
 *
 * @tparam Arguments the type of the expected arguments object
 */
case class Command[+Arguments](name: String, arguments: Arguments)
case class Definition(language: String, serialization: String, data: String)
case class Operation(name: String, definition: Option[Definition])
case class Partial[+Arguments](operation: Operation, arguments: Arguments)

//case class View(id: Long, basedOn: Long,
//                name: String, schema: Schema, transform: Transform) extends HasId {
//  require(id > 0)
//  require(name != null)
//  require(name.trim.length > 0)
//  require(schema != null)
//  require(schema.columns.length > 0)
//}
