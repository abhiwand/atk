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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.frame.{ FrameReferenceManagement, FrameReference }
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.engine.spark.command.{ Typeful, Dependencies }
import org.scalatest.{ FlatSpec, Matchers }
import spray.json._

class DependenciesTest extends FlatSpec with Matchers {

  "getUriReferencesForJson" should "find UriReferences in case classes" in {
    case class Foo(frameId: Int, frame: FrameReference)
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    implicit val fmt = jsonFormat2(Foo)

    val reference = List(FrameReference(3, None))
    implicit val invocation: Invocation = Call(null)
    FrameReferenceManagement //reference to ensure it's loaded and registered
    Dependencies.getUriReferencesFromJsObject(Foo(1, reference.head).toJson.asJsObject) should be(reference)
  }
}
