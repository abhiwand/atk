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

package com.intel.intelanalytics.rest.v1.viewmodels

import com.intel.intelanalytics.domain.command.CommandDefinition
import com.intel.intelanalytics.domain.frame.FrameReference
import spray.httpx.SprayJsonSupport
import spray.json._
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol

/**
 * Implicit Conversions for View/Models to JSON
 */
object ViewModelJsonImplicits extends IADefaultJsonProtocol with SprayJsonSupport {

  //this is needed for implicits
  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val relLinkFormat = jsonFormat3(RelLink)
  implicit val getCommandsFormat = jsonFormat3(GetCommands)
  implicit val getCommandFormat = jsonFormat9(GetCommand)
  implicit val getDataFramesFormat = jsonFormat4(GetDataFrames)
  implicit val getDataFrameFormat = jsonFormat9(GetDataFrame)
  implicit val getGraphsFormat = jsonFormat4(GetGraphs)
  implicit val getGraphFormat = jsonFormat6(GetGraph)
  implicit val getModelFormat = jsonFormat6(GetModel)
  implicit val getModelsFormat = jsonFormat4(GetModels)
  implicit val getQueryPageFormat = jsonFormat4(GetQueryPage)
  implicit val getQueryPagesFormat = jsonFormat2(GetQueryPages)
  implicit val getQueriesFormat = jsonFormat3(GetQueries)
  implicit val getQueryFormat = jsonFormat8(GetQuery)
  implicit val jsonTransformFormat = jsonFormat2(JsonTransform)
}