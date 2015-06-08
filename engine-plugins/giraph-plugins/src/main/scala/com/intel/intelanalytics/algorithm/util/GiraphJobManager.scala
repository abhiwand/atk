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

package com.intel.intelanalytics.algorithm.util

import java.io.File

import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.{ CommandStorage, ProgressInfo }
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, Invocation }
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.{ DefaultJobObserver, GiraphJob }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.{ Job }
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
      _ =>
        {
          val conf = submittedJob.getConfiguration()
          val str = conf.get("giraphjob.maxSteps")
          var maxSteps: Float = 20
          if (str != null) {
            maxSteps = str.toInt + 4 //4 for init, input, step and shutdown
          }
          val group = submittedJob.getCounters().getGroup("Giraph Timers")
          if (null != group) {
            var progress = (group.size() - 1) / maxSteps
            if (progress > 0.95) progress = 0.95f //each algorithm calculates steps differently and this sometimes cause it to be greater than 1. It is easier to fix it here
            commandStorage.updateProgress(commandId, List(ProgressInfo(progress * 100, None)))
          }
          else {
            commandStorage.updateProgress(commandId, List(ProgressInfo(submittedJob.mapProgress() * 100, None)))
          }
        }
    }
  }

  override def jobFinished(jobToSubmit: Job, passed: Boolean) = {
    val commandId = getCommandId(jobToSubmit)
    GiraphJobListener.commandIdMap.-(commandId)
    println(jobToSubmit.toString)
    if (!jobToSubmit.isSuccessful) {
      val taskCompletionEvents = jobToSubmit.getTaskCompletionEvents(0)
      taskCompletionEvents.lastOption match {
        case Some(e) =>
          val diagnostics = jobToSubmit.getTaskDiagnostics(e.getTaskAttemptId)(0)
          val errorMessage = diagnostics.lastIndexOf("Caused by:") match {
            case index if index > 0 => diagnostics.substring(index)
            case _ => diagnostics
          }
          throw new Exception(s"Execution was unsuccessful. $errorMessage")
        case None => throw new Exception("Execution was unsuccessful, but no further information was provided. " +
          "Consider checking server logs for further information.")
      }
    }
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

    val giraphLoader = Boot.getArchive(config.getString("giraph.archive.name")).classLoader
    Thread.currentThread().setContextClassLoader(giraphLoader)

    val commandInvocation = invocation.asInstanceOf[CommandInvocation]

    GiraphJobListener.commandIdMap(commandInvocation.commandId) = commandInvocation.commandStorage
    giraphConf.setJobObserverClass(classOf[GiraphJobListener])

    giraphConf.setLong("giraph.ml.commandId", commandInvocation.commandId)

    val job = new GiraphJob(giraphConf, jobName)
    val internalJob: Job = job.getInternalJob

    // Clear Giraph Report Directory
    val fs = FileSystem.get(new Configuration())
    val output_dir_path = config.getString("fs.root") +
      File.separator +
      config.getString("output.dir") +
      File.separator +
      commandInvocation.commandId
    if (config.getBoolean("output.overwrite")) {
      fs.delete(getFullyQualifiedPath(output_dir_path, fs), true)
    }

    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      getFullyQualifiedPath(output_dir_path, fs))

    job.run(true) match {
      case false => "Error: No Learning Report found!!"
      case true =>
        val stream = fs.open(getFullyQualifiedPath(output_dir_path + File.separator + reportName, fs))
        def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
        val result = readLines.takeWhile(_ != null).toList.mkString("\n")
        result
    }
  }

}
