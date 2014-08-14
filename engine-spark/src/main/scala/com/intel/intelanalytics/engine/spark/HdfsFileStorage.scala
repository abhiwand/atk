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

import java.io.{ IOException, InputStream, OutputStream }
import java.nio.file.{ Path, Paths }

import com.intel.intelanalytics.engine.{ Directory, Entry, File, FileStorage }
import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path => HPath, FileUtil, FileSystem, LocalFileSystem }
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.net.URI

class HdfsFileStorage(fsRoot: String) extends FileStorage with EventLogging {

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
   * HPath from a path
   * @param path a path relative to the root or that includes the root
   */
  private def absoluteHPath(path: String): HPath = {
    if (path.startsWith(fsRoot)) {
      new HPath(path)
    }
    else {
      new HPath(concatPaths(fsRoot, path))
    }
  }

  override def write(sink: File, append: Boolean): OutputStream = withContext("file.write") {
    val path: HPath = absoluteHPath(sink.path.toString)
    if (append) {
      fs.append(path)
    }
    else {
      fs.create(path, true)
    }
  }

  override def list(source: Directory): Seq[Entry] = withContext("file.list") {
    fs.listStatus(absoluteHPath(source.path.toString))
      .map {
        case s if s.isDirectory => Directory(path = Paths.get(s.getPath.toString))
        case f if f.isDirectory => File(path = Paths.get(f.getPath.toString), size = f.getLen)
        case x => throw new IOException("Unknown object type in filesystem at " + x.getPath)
      }
  }

  override def read(source: File): InputStream = withContext("file.read") {
    val path: HPath = absoluteHPath(source.path.toString)
    fs.open(path)
  }

  override def exists(path: URI): Boolean = withContext("file.exists") {
    val p: HPath = absoluteHPath(path.toString)
    fs.exists(p)
  }

  /**
   * Recursive delete
   */
  override def delete(path: URI): Unit = withContext("file.delete") {
    val fullPath = absoluteHPath(path.toString)
    if (fs.exists(fullPath)) {
      val success = fs.delete(fullPath, true) // recursive
      if (!success) {
        error("Could not delete path: " + fullPath.toUri)
      }
    }
  }

  override def create(file: Path): Unit = withContext("file.create") {
    fs.create(absoluteHPath(file.toString))
  }

  override def createDirectory(directory: URI): Unit = withContext("file.createDirectory") {
    val adjusted = absoluteHPath(directory.toString)
    fs.mkdirs(adjusted)
  }

  /**
   * File size
   * @param path relative path
   */
  override def size(path: String): Long = {
    val p: HPath = absoluteHPath(path.toString)
    fs.getFileStatus(p).getLen
  }
}

