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

package com.intel.intelanalytics.algorithm.util

import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.{ CommandStorage, ProgressInfo }
import com.intel.intelanalytics.engine.plugin.Invocation
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.{ DefaultJobObserver, GiraphJob }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.Job
import com.typesafe.config.Config
import scala.collection.mutable.HashMap

import scala.concurrent._

object GiraphJobListener {
  var commandIdMap = new HashMap[Long, CommandStorage]
}

/**
 * GiraphJobListener overrides jobRunning method which gets called after the internal hadoop job has been submitted
 * We update the progress for the commandId to the commandStorage periodically until the job is complete
 */
class GiraphJobListener extends DefaultJobObserver {

  override def launchingJob(jobToSubmit: Job) = {
    val commandId = getCommandId(jobToSubmit)
    val commandStorage = getCommandStorage(commandId)
    commandStorage.updateProgress(commandId, List(ProgressInfo(0.0f, None)))
  }

  override def jobRunning(submittedJob: Job) = {
    val commandId = getCommandId(submittedJob)
    val commandStorage = getCommandStorage(commandId)
    Stream.continually(submittedJob.isComplete).takeWhile(_ == false).foreach {
      _ => commandStorage.updateProgress(commandId, List(ProgressInfo(submittedJob.mapProgress() * 100, None)))
    }
  }

  override def jobFinished(jobToSubmit: Job, passed: Boolean) = {
    val commandId = getCommandId(jobToSubmit)
    GiraphJobListener.commandIdMap.-(commandId)
    println(jobToSubmit.toString)
  }

  private def getCommandId(job: Job): Long = job.getConfiguration.getLong("giraph.ml.commandId", 0)
  private def getCommandStorage(commandId: Long): CommandStorage = GiraphJobListener.commandIdMap.getOrElse(commandId, null)

}

/**
 * GiraphManager invokes the Giraph Job and waits for completion. Upon completion - it reads and returns back the
 * report to caller
 */
object GiraphJobManager {

  def getFullyQualifiedPath(path: String, fs: FileSystem): Path =
    fs.makeQualified(Path.getPathWithoutSchemeAndAuthority(new Path(path)))

  def run(jobName: String, computationClassCanonicalName: String,
          config: Config, giraphConf: GiraphConfiguration, invocation: Invocation, reportName: String): String = {

    val giraphLoader = Boot.getClassLoader(config.getString("giraph.archive.name"))
    Thread.currentThread().setContextClassLoader(giraphLoader)

    GiraphJobListener.commandIdMap(invocation.commandId) = invocation.commandStorage
    giraphConf.setJobObserverClass(classOf[GiraphJobListener])

    giraphConf.setLong("giraph.ml.commandId", invocation.commandId)

    val job = new GiraphJob(giraphConf, jobName)
    val internalJob: Job = job.getInternalJob

    // Clear Giraph Report Directory
    val fs = FileSystem.get(new Configuration())
    val output_dir_path = config.getString("fs.root") + "/" + config.getString("output.dir") + "/" + invocation.commandId
    if (config.getBoolean("output.overwrite")) {
      fs.delete(getFullyQualifiedPath(output_dir_path, fs), true)
    }

    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      getFullyQualifiedPath(output_dir_path, fs))

    job.run(true) match {
      case false => "Error: No Learning Report found!!"
      case true =>
        val stream = fs.open(getFullyQualifiedPath(output_dir_path + "/" + reportName, fs))
        def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
        val result = readLines.takeWhile(_ != null).toList.mkString("\n")
        fs.close()
        result
    }
  }

}
