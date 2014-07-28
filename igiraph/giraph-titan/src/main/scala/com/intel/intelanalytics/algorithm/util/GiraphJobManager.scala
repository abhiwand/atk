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
import com.intel.intelanalytics.engine.ProgressInfo
import com.intel.intelanalytics.engine.plugin.Invocation
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.{ DefaultJobObserver, GiraphJob }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.Job
import com.typesafe.config.Config
import java.net.URI

import scala.concurrent._

object GiraphJobListener {
  var invocation: Invocation = null
}

/**
 * GiraphJobListener overrides jobRunning method which gets called after the internal hadoop job has been submitted
 * We update the progress for the commandId to the commandStorage periodically until the job is complete
 */
class GiraphJobListener extends DefaultJobObserver {

  override def jobRunning(submittedJob: Job) = {
    implicit val ec = GiraphJobListener.invocation.executionContext
    val commandId = GiraphJobListener.invocation.commandId
    val commandStorage = GiraphJobListener.invocation.commandStorage
    Future {
      Stream.continually(submittedJob.isComplete).takeWhile(_ == false).foreach {
        _ => commandStorage.updateProgress(commandId, List(ProgressInfo(submittedJob.mapProgress() * 100, None)))
      }
    }
  }
}

/**
 * GiraphManager invokes the Giraph Job and waits for completion. Upon completion - it reads and returns back the
 * report to caller
 */
object GiraphJobManager {

  def run(jobName: String, computationClassCanonicalName: String,
          config: Config, giraphConf: GiraphConfiguration, invocation: Invocation, reportName: String): String = {

    val giraphLoader = Boot.getClassLoader(config.getString("giraph.archive.name"))
    Thread.currentThread().setContextClassLoader(giraphLoader)

    GiraphJobListener.invocation = invocation
    giraphConf.setJobObserverClass(classOf[GiraphJobListener])

    val job = new GiraphJob(giraphConf, jobName)
    val internalJob: Job = job.getInternalJob

    // Clear Giraph Report Directory
    val output_dir_path = config.getString("fs.root") + "/" + config.getString("output.dir") + "/" + invocation.commandId
    val output_dir = new URI(output_dir_path)
    if (config.getBoolean("output.overwrite")) {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path(output_dir), true)
    }

    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      new Path(output_dir))

    job.run(true) match {
      case false => "Error: No Learning Report found!!"
      case true =>
        val fs = FileSystem.get(new Configuration())
        val stream = fs.open(new Path(output_dir_path + "/" + reportName))
        def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
        val result = readLines.takeWhile(_ != null).toList.mkString("\n")
        fs.close()
        result
    }
  }

}