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

package com.intel.intelanalytics.engine.spark.graph.plugins

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.graphbuilder.elements.Property

class ExportFromTitanToParquetGraphTest extends FlatSpec with Matchers {
  "getPropertiesValueByColumns" should "get property values by column sequence" in {
    val properties = Set(Property("col4", 2f), Property("col1", 1), Property("col2", "2"), Property("col3", true))
    val result = ExportFromTitanToParquetGraph.getPropertiesValueByColumns(List("col1", "col2", "col3", "col4"), properties)
    result shouldBe Array(1, "2", true, 2f)
  }
}
