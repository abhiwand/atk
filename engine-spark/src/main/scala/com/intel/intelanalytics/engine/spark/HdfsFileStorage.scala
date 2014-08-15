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

package com.intel.intelanalytics.engine.spark

import java.io.{InputStream, OutputStream}

import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, LocalFileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem

class HdfsFileStorage(fsRoot: String) extends EventLogging {

  val configuration = withContext("HDFSFileStorage.configuration") {

    info("fsRoot: " + fsRoot)

    val hadoopConfig = new Configuration()
    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    hadoopConfig.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[LocalFileSystem].getName)

    if (fsRoot.startsWith("hdfs")) {
      info("fsRoot starts with HDFS")
      hadoopConfig.set("fs.defaultFS", fsRoot)
    }

    require(hadoopConfig.getClassByNameOrNull(classOf[LocalFileSystem].getName) != null,
      "Could not load local filesystem for Hadoop")
    hadoopConfig
  }

  val fs = FileSystem.get(configuration)

  /**
   * Path from a path
   * @param path a path relative to the root or that includes the root
   */
  private[spark] def absolutePath(path: String): Path = {
    // TODO: this seems to work but this could be revisted and perhaps done nicer
    if (path.startsWith(fsRoot)) {
      new Path(path)
    }
    else {
      new Path(concatPaths(fsRoot, path))
    }
  }

  def write(sink: Path, append: Boolean): OutputStream = withContext("file.write") {
    val path: Path = absolutePath(sink.toString)
    if (append) {
      fs.append(path)
    }
    else {
      fs.create(path, true)
    }
  }

  def list(source: Path): Seq[Path] = withContext("file.list") {
    fs.listStatus(absolutePath(source.toString)).map(fs => fs.getPath)
  }

  def read(source: Path): InputStream = withContext("file.read") {
    val path: Path = absolutePath(source.toString)
    fs.open(path)
  }

  def exists(path: Path): Boolean = withContext("file.exists") {
    val p: Path = absolutePath(path.toString)
    fs.exists(p)
  }

  /**
   * Delete
   * @param recursive true to delete subdirectories and files
   */
  def delete(path: Path, recursive: Boolean = true): Unit = withContext("file.delete") {
    val fullPath = absolutePath(path.toString)
    if (fs.exists(fullPath)) {
      val success = fs.delete(fullPath, recursive)
      if (!success) {
        error("Could not delete path: " + fullPath.toUri.toString)
      }
    }
  }

  def create(file: Path): Unit = withContext("file.create") {
    fs.create(absolutePath(file.toString))
  }

  def createDirectory(directory: Path): Unit = withContext("file.createDirectory") {
    val adjusted = absolutePath(directory.toString)
    fs.mkdirs(adjusted)
  }

  /**
   * File size
   * @param path relative path
   */
  def size(path: String): Long = {
    val p: Path = absolutePath(path)
    fs.getFileStatus(p).getLen
  }
}

