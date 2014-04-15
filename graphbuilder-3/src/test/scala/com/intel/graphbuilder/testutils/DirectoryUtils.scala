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
