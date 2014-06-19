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

import com.intel.intelanalytics.domain.schema.DataTypes
import org.scalatest.{FlatSpec, Matchers}

class FrameAppendTest extends FlatSpec with Matchers {
  "List[DataTypes]" should "determine which type they will combine into" in {
    DataTypes.mergeTypes(DataTypes.string :: DataTypes.int32 :: DataTypes.float64 :: Nil) should be(DataTypes.string)
    DataTypes.mergeTypes(DataTypes.string :: DataTypes.float64 :: Nil) should be(DataTypes.string)
    DataTypes.mergeTypes(DataTypes.string :: DataTypes.int64 :: DataTypes.float64 :: Nil) should be(DataTypes.string)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.float64 :: Nil) should be(DataTypes.float64)
    DataTypes.mergeTypes(DataTypes.int64 :: DataTypes.float32 :: Nil) should be(DataTypes.float64)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.int64 :: Nil) should be(DataTypes.int64)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.float32 :: Nil) should be(DataTypes.float32)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.int32 :: Nil) should be(DataTypes.int32)
  }

}
