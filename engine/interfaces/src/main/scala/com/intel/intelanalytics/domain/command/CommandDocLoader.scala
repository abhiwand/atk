/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
