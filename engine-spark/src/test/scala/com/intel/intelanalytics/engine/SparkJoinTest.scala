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

package com.intel.intelanalytics.engine.spark

import org.scalatest._
import com.intel.intelanalytics.domain.DataTypes.{ string, int32, DataType }

class SparkJoinTest extends FlatSpec with Matchers {

  "create2TupleForJoin" should "put first column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 0)
    result._1 shouldBe "1"
    result._2 shouldBe Array("1", "2", 3, 4, "5")
  }

  "create2TupleForJoin" should "put third column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 2)
    result._1 shouldBe 3
    result._2 shouldBe Array("1", "2", 3, 4, "5")
  }

  "join schema" should "keep field name the same when there is not name conflicts" in {
    val leftCols: List[(String, DataType)] = List(("a", int32), ("b", string))
    val rightCols: List[(String, DataType)] = List(("c", int32), ("d", string))
    val result = SparkEngine.resolveSchemaNamingConflicts(leftCols, rightCols)
    leftCols shouldBe result._1
    rightCols shouldBe result._2
  }

  "join schema" should "append l to the field from left and r to the field from right if field names conflict" in {
    val leftCols: List[(String, DataType)] = List(("a", int32), ("b", string), ("c", string))
    val rightCols: List[(String, DataType)] = List(("a", int32), ("c", string), ("d", string))
    val result = SparkEngine.resolveSchemaNamingConflicts(leftCols, rightCols)
    List(("a_l", int32), ("b", string), ("c_l", string)) shouldBe result._1
    List(("a_r", int32), ("c_r", string), ("d", string)) shouldBe result._2
  }
}
