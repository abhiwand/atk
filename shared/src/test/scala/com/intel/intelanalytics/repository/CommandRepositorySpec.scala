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

package com.intel.intelanalytics.repository

import org.scalatest.Matchers
import com.intel.intelanalytics.domain.command.CommandTemplate

class CommandRepositorySpec extends SlickMetaStoreH2Testing with Matchers {

  "CommandRepository" should "be able to create commands" in {

    val commandRepo = slickMetaStoreComponent.metaStore.commandRepo

    slickMetaStoreComponent.metaStore.withSession("command-test") {
      implicit session ⇒

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
