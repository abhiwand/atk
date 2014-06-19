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

package com.intel.intelanalytics.service.v1.decorators

import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class CommandDecoratorSpec extends FlatSpec with Matchers {

  val uri = "http://www.example.com/commands"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val command = new Command(1, "name", None, None, false, None, new DateTime, new DateTime)

  "CommandDecorator" should "be able to decorate a command" in {
    val decoratedCommand = CommandDecorator.decorateEntity(null, relLinks, command)
    decoratedCommand.id should be(1)
    decoratedCommand.name should be("name")
    decoratedCommand.links.head.uri should be("http://www.example.com/commands")
  }

  it should "set the correct URL in decorating a list of commands" in {
    val commandHeaders = CommandDecorator.decorateForIndex(uri, Seq(command))
    val commandHeader = commandHeaders.toList.head
    commandHeader.url should be("http://www.example.com/commands/1")
  }
}
