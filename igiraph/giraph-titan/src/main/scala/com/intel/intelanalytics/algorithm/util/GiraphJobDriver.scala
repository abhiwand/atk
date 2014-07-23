package com.intel.intelanalytics.algorithm.util

import com.intel.intelanalytics.component.Boot
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.GiraphJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.Job
import com.typesafe.config.Config
import java.net.URI

object GiraphJobDriver {

  def run(jobName: String, computationClassCanonicalName: String,
          config: Config, giraphConf: GiraphConfiguration, commandId: Long, reportName: String): String = {

    val giraphLoader = Boot.getClassLoader(config.getString("giraph.archive.name"))
    Thread.currentThread().setContextClassLoader(giraphLoader)

    val job = new GiraphJob(giraphConf, jobName)
    val internalJob: Job = job.getInternalJob
    val algorithm = giraphLoader.loadClass(computationClassCanonicalName)
    internalJob.setJarByClass(algorithm)

    val output_dir_path = config.getString("fs.root") + "/" + config.getString("output.dir") + "/" + commandId
    val output_dir = new URI(output_dir_path)
    if (config.getBoolean("output.overwrite")) {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path(output_dir), true)
    }

    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      new Path(output_dir))

    if (job.run(true)) {
      val fs = FileSystem.get(new Configuration())
      val stream = fs.open(new Path(output_dir_path + "/" + reportName))
      def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
      val result = readLines.takeWhile(_ != null).toList.mkString("\n")
      fs.close()
      result
    }
    else "Error: No Learning Report found!!"
  }

}