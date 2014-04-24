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

package com.intel.graphbuilder.testutils

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

/**
 * Utility methods for working with directories.
 */
object DirectoryUtils {

  private val log: Logger = Logger.getLogger(DirectoryUtils.getClass)

  /**
   * Create a Temporary directory
   * @param prefix the prefix for the directory name, this is used to make the Temp directory more identifiable.
   * @return the temporary directory
   */
  def createTempDirectory(prefix: String): File = {
    try {
      convertFileToDirectory(File.createTempFile(prefix, "-tmp"))
    }
    catch {
      case e: Exception =>
        throw new RuntimeException("Could NOT initialize temp directory, prefix: " + prefix, e)
    }
  }

  /**
   * Convert a file into a directory
   * @param file a file that isn't a directory
   * @return directory with same name as File
   */
  private def convertFileToDirectory(file: File): File = {
    file.delete()
    if (!file.mkdirs()) {
      throw new RuntimeException("Failed to create tmpDir: " + file.getAbsolutePath)
    }
    file
  }

  def deleteTempDirectory(tmpDir: File) {
    FileUtils.deleteQuietly(tmpDir)
    if (tmpDir != null && tmpDir.exists) {
      log.error("Failed to delete tmpDir: " + tmpDir.getAbsolutePath)
    }
  }
}
