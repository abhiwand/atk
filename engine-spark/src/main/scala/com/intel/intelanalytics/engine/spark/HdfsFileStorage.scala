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

import com.intel.intelanalytics.engine.FileComponent.FileStorage
import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.fs.{Path, FileSystem, LocalFileSystem}
import com.intel.intelanalytics.engine.FileComponent.File
import java.io.{IOException, InputStream, OutputStream}
import com.intel.intelanalytics.engine.Rows.RowSource
import com.intel.intelanalytics.engine.FileComponent.Directory
import com.intel.intelanalytics.engine.FileComponent.Entry
import java.nio.file.{Path, Paths}
import org.apache.hadoop.fs.Path

trait HdfsFileStorage extends FileStorage with EventLogging {

  val configuration = {
    val hadoopConfig = new Configuration()
    require(hadoopConfig.getClass().getClassLoader == this.getClass.getClassLoader)
    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    hadoopConfig.set("fs.hdfs.impl",
      classOf[DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl",
      classOf[LocalFileSystem].getName)
    require(hadoopConfig.getClassByNameOrNull(classOf[LocalFileSystem].getName) != null)
    hadoopConfig
  }

  val fs = FileSystem.get(configuration)

  override def write(sink: File, append: Boolean): OutputStream = withContext("file.write") {
    val path: HPath = new HPath(fsRoot + sink.path.toString)
    if (append) {
      fs.append(path)
    }
    else {
      fs.create(path, true)
    }
  }

  override def readRows(source: File, rowGenerator: (InputStream) => RowSource,
                        offsetBytes: Long, readBytes: Long): Unit = withContext("file.readRows") {
    ???
  }

  override def list(source: Directory): Seq[Entry] = withContext("file.list") {
    fs.listStatus(new HPath(fsRoot + frames.frameBase))
      .map {
      //case s if s.isDirectory => Directory(path = Paths.get(s.getPath.toString))
      case s if s.isDirectory => Directory(path = Paths.get(s.getPath.toString))
      case f if f.isDirectory => File(path = Paths.get(f.getPath.toString), size = f.getLen)
      case x => throw new IOException("Unknown object type in filesystem at " + x.getPath)
    }
  }

  override def read(source: File): InputStream = withContext("file.read") {
    val path: HPath = new HPath(fsRoot + source.path.toString)
    fs.open(path)
  }

  //TODO: switch file methods to strings instead of Path?
  override def copy(source: Path, destination: Path): Unit = withContext("file.copy") {
    ???
  }

  override def move(source: Path, destination: Path): Unit = withContext("file.move") {
    ???
  }

  override def getMetaData(path: Path): Option[Entry] = withContext("file.getMetaData") {
    val hPath: HPath = new HPath(fsRoot + path.toString)
    val exists = fs.exists(hPath)
    if (!exists) {
      None
    }
    else {
      val status = fs.getStatus(hPath)
      if (status == null || fs.isDirectory(hPath)) {
        Some(Directory(path))
      }
      else {
        Some(File(path, status.getUsed))
      }
    }
  }

  override def delete(path: Path): Unit = withContext("file.delete") {
    fs.delete(new HPath(fsRoot + path.toString), true)
  }

  override def create(file: Path): Unit = withContext("file.create") {
    fs.create(new HPath(fsRoot + file.toString))
  }

  override def createDirectory(directory: Path): Directory = withContext("file.createDirectory") {
    val adjusted = fsRoot + directory.toString
    fs.mkdirs(new HPath(adjusted))
    getMetaData(Paths.get(directory.toString)).get.asInstanceOf[Directory]
  }
}

