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

package com.intel.intelanalytics.service.v1.viewmodels

import com.intel.intelanalytics.domain.command.CommandDefinition
import com.intel.intelanalytics.domain.frame.FrameReference
import spray.httpx.SprayJsonSupport
import spray.json._

/**
 * Implicit Conversions for View/Models to JSON
 */
object ViewModelJsonImplicits extends DefaultJsonProtocol with SprayJsonSupport {

  //this is needed for implicits
  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val relLinkFormat = jsonFormat3(RelLink)
  implicit val getCommandsFormat = jsonFormat3(GetCommands)
  implicit val getCommandFormat = jsonFormat9(GetCommand)
  implicit val getDataFramesFormat = jsonFormat3(GetDataFrames)
  implicit val getDataFrameFormat = jsonFormat4(GetDataFrame)
  implicit val getGraphsFormat = jsonFormat3(GetGraphs)
  implicit val getGraphFormat = jsonFormat3(GetGraph)
  implicit val getQueryPageFormat = jsonFormat3(GetQueryPage)
  implicit val getQueryPagesFormat = jsonFormat2(GetQueryPages)
  implicit val getQueriesFormat = jsonFormat3(GetQueries)
  implicit val getQueryFormat = jsonFormat7(GetQuery)
  implicit val jsonTransformFormat = jsonFormat2(JsonTransform)
}