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
package com.intel.intelanalytics.domain.command

import com.intel.intelanalytics.apidoc.CommandDocText

/**
 * Creates CommandDoc objects by loading resource files
 */
object CommandDocLoader {

  /**
   * Creates a CommandDoc for the given command name from resource file
   * @param commandName full name of the command, like "frame/add_columns"
   * @return CommandDoc, None if command not found in resource files
   */
  def getCommandDoc(commandName: String): Option[CommandDoc] = {
    val path = getPath(commandName)
    val text = CommandDocText.getText(path, "python")
    createCommandDoc(text)
  }

  /**
   * Creates a CommandDoc from RST text
   * @param text RST text of Command API doc
   * @return  CommandDoc, returns None if None given
   */
  private def createCommandDoc(text: Option[String]): Option[CommandDoc] = {
    text match {
      case Some(t) =>
        val oneLineSummary = t.lines.next()
        val extendedSummary = t.drop(oneLineSummary.size)
        Some(CommandDoc(oneLineSummary.trim, Some(extendedSummary)))
      case None => None
    }
  }

  private def getPath(commandName: String): String = {
    commandName.replace(':', '-') // e.g.  "frame:vertex/count" is "frame-vertex/count" in the dir structure
  }
}
