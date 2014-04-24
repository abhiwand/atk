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

package com.intel.graphbuilder.parser

/**
 * Parser parses objects of type T from rows using the supplied InputSchema.
 */
abstract class Parser[T](inputSchema: InputSchema) extends Serializable {

  /**
   * Parse a row of data into zero to many T
   *
   * Random access is needed so preferably an IndexedSeq[String] is supplied
   */
  def parse(row: Seq[Any]): Seq[T] = {
    parse(new InputRow(inputSchema, row))
  }


  /**
   * Parse a row of data into zero to many T
   */
  def parse(row: InputRow): Seq[T]
}