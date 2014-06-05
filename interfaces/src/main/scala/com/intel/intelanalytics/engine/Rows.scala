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

import java.nio.file.Path
import scala.concurrent.Future
import java.io.{ OutputStream, InputStream }
import com.intel.intelanalytics.engine.Rows.Row
import spray.json.JsObject
import scala.util.Try
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.command._
import FrameRenameColumn
import FrameProject
import FrameRenameFrame
import CommandTemplate
import FilterPredicate
import com.intel.intelanalytics.security.UserPrincipal
import FrameRemoveColumn
import GraphLoad
import LoadLines
import Command
import com.intel.intelanalytics.domain.command.Als
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.graph.{GraphLoad, Graph}
import FrameProject
import com.intel.intelanalytics.domain.graph.Graph
import FilterPredicate
import FrameAddColumn
import CommandTemplate
import com.intel.intelanalytics.security.UserPrincipal
import FrameRemoveColumn
import FrameJoin
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.graph.GraphTemplate
import Command
import com.intel.intelanalytics.domain.graph.Graph
import FilterPredicate
import CommandTemplate
import FlattenColumn
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.graph.GraphTemplate
import Command
import com.intel.intelanalytics.domain.command.Als
import com.intel.intelanalytics.domain.frame.FrameProject
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.frame.FrameRenameFrame
import FilterPredicate
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import CommandTemplate
import com.intel.intelanalytics.domain.frame.FlattenColumn
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.frame.LoadLines
import Command
import com.intel.intelanalytics.domain.command.Als
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.Schema

object Rows {
  type Row = Array[Any]

  //TODO: Can we constrain this better?
  trait RowSource {
    def schema: Schema

    def rows: Iterable[Row]
  }

}

//object EngineMessages {
//  case class AppendFile(id: Long, fileName: String, rowGenerator: Functional)
//  case class DropColumn(id: Long, name: String)
//  case class AddColumn(id: Long, name: String, map: Option[RowFunction[Any]])
//  case class DropRows(id: Long, filter: RowFunction[Boolean])
//}















// TODO: Move classes from cake to dependency injection.




