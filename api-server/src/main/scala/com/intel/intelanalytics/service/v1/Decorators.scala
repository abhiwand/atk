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
package com.intel.intelanalytics.service.v1

import com.intel.intelanalytics.domain.{ Command, DataFrame }
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedCommand
import com.intel.intelanalytics.service.v1.viewmodels.DataFrameHeader
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.intel.intelanalytics.domain.Command
import com.intel.intelanalytics.domain.{ Graph, DataFrame }
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.domain.Graph
import com.intel.intelanalytics.service.v1.viewmodels.GraphHeader
import com.intel.intelanalytics.service.v1.viewmodels.DataFrameHeader
import com.intel.intelanalytics.domain.DataFrame
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedDataFrame

trait EntityDecorator[Entity, Index, Decorated] {
  def decorateForIndex(indexUri: String, entities: Seq[Entity]): List[Index]
  def decorateEntity(uri: String, links: Iterable[RelLink], entity: Entity): Decorated
}

object FrameDecorator extends EntityDecorator[DataFrame, DataFrameHeader, DecoratedDataFrame] {
  override def decorateEntity(uri: String,
                              links: Iterable[RelLink],
                              entity: DataFrame): DecoratedDataFrame = {
    DecoratedDataFrame(id = entity.id, name = entity.name,
      schema = entity.schema, links = links.toList)
  }

  override def decorateForIndex(uri: String, entities: Seq[DataFrame]): List[DataFrameHeader] = {
    entities.map(frame => new DataFrameHeader(id = frame.id,
      name = frame.name,
      url = uri + "/" + frame.id)).toList
  }
}

object CommandDecorator extends EntityDecorator[Command, CommandHeader, DecoratedCommand] {
  override def decorateEntity(uri: String,
                              links: Iterable[RelLink],
                              entity: Command): DecoratedCommand = {
    DecoratedCommand(id = entity.id, name = entity.name,
      arguments = entity.arguments, error = entity.error, complete = entity.complete,
      links = links.toList)
  }

  override def decorateForIndex(uri: String, entities: Seq[Command]): List[CommandHeader] = {
    entities.map(frame => new CommandHeader(id = frame.id,
      name = frame.name,
      url = uri + "/" + frame.id)).toList
  }
}

object GraphDecorator extends EntityDecorator[Graph, GraphHeader, DecoratedGraph] {
  override def decorateEntity(uri: String,
                              links: Iterable[RelLink],
                              entity: Graph): DecoratedGraph = {
    DecoratedGraph(id = entity.id, name = entity.name, links = links.toList)
  }

  override def decorateForIndex(uri: String, entities: Seq[Graph]): List[GraphHeader] = {
    entities.map(graph => new GraphHeader(id = graph.id,
      name = graph.name,
      url = uri + "/" + graph.id)).toList
  }

}
