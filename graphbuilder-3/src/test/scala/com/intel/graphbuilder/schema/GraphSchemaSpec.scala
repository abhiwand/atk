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

package com.intel.graphbuilder.schema

import org.specs2.mutable.Specification

class GraphSchemaSpec extends Specification {

  "GraphSchema" should {

    "be able to provide properties by name" in {
      val propDef1 = new PropertyDef(PropertyType.Vertex, "one", classOf[String], true, true)
      val propDef2 = new PropertyDef(PropertyType.Vertex, "two", classOf[String], false, false)
      val propDef3 = new PropertyDef(PropertyType.Edge, "three", classOf[String], false, false)
      val propDef4 = new PropertyDef(PropertyType.Vertex, "one", classOf[String], false, false)

      // invoke method under test
      val schema = new GraphSchema(Nil, List(propDef1, propDef2, propDef3, propDef4))

      // validations
      schema.propertiesWithName("one").size mustEqual 2
      schema.propertiesWithName("two").size mustEqual 1
      schema.propertiesWithName("three").size mustEqual 1
      schema.propertiesWithName("three").head.propertyType mustEqual PropertyType.Edge
    }
  }
}
