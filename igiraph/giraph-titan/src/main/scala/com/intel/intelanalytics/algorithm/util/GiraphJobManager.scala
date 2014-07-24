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