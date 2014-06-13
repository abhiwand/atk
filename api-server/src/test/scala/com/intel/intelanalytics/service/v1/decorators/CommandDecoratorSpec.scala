package com.intel.intelanalytics.service.v1.decorators

import org.scalatest.{Matchers, FlatSpec}
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import org.joda.time.DateTime
import com.intel.intelanalytics.domain.command.Command

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
