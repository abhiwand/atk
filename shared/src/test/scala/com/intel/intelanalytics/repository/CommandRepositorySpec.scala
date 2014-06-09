package com.intel.intelanalytics.repository

import org.scalatest.Matchers
import com.intel.intelanalytics.domain.CommandTemplate

class CommandRepositorySpec extends SlickMetaStoreH2Testing with Matchers {

  "CommandRepository" should "be able to create commands" in {

    val commandRepo = slickMetaStoreComponent.metaStore.commandRepo

    slickMetaStoreComponent.metaStore.withSession("command-test") {
      implicit session =>

        val name = "my-name"

        // create a command
        val command = commandRepo.insert(new CommandTemplate(name, None))
        command.get.name shouldBe name

        // look it up and validate expected values
        val command2 = commandRepo.lookup(command.get.id)
        command.get shouldBe command2.get
        command2.get.name shouldBe name
        command2.get.createdOn should not be null
        command2.get.modifiedOn should not be null
    }
  }
}
