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

package com.intel.taproot.analytics.scoring

import java.io.{ FileOutputStream, File, FileInputStream }

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.io.IO
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.intel.taproot.event.{ EventLogging, EventLogger }
import com.intel.taproot.analytics.component.{ ArchiveDefinition, Archive }
import com.typesafe.config.{ Config, ConfigFactory }
import scala.reflect.ClassTag
import com.intel.taproot.analytics.scoring.interfaces.{ Model, ModelLoader }

/**
 * Scoring Service Application - a REST application used by client layer to communicate with the Model.
 *
 * See the 'scoring_server.sh' to see how the launcher starts the application.
 */
class ScoringServiceApplication(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) with EventLogging {

  EventLogging.raw = true
  info("Scoring server setting log adapter from configuration")

  EventLogging.raw = configuration.getBoolean("intel.taproot.analytics.scoring.logging.raw")
  info("Scoring server set log adapter from configuration")

  EventLogging.profiling = configuration.getBoolean("intel.taproot.analytics.scoring.logging.profile")
  info(s"Scoring server profiling: ${EventLogging.profiling}")

  //Direct subsequent archive messages to the normal log
  Archive.logger = s => info(s)
  Archive.logger("Archive logger installed")

  var archiveName: String = null
  var modelName: String = null
  var ModelBytesFileName: String = null

  /**
   * Main entry point to start the Scoring Service Application
   */
  override def start() = {

    readModelInfo()
    //
    lazy val modelLoader = com.intel.taproot.analytics.component.Boot.getArchive(archiveName)
      .load("com.intel.taproot.analytics.scoring.models." + config.getString("intel.taproot.scoring-engine.scoring.loader"))
    //
    //    //val modelFile = config.getString("intel.taproot.scoring-engine.scoring.model")
    //lazy val modelLoader = com.intel.taproot.analytics.component.Boot.getArchive(config.getString("intel.taproot.scoring-engine.archive"))
    //.load("com.intel.taproot.analytics.scoring.models." + config.getString("intel.taproot.scoring-engine.scoring.loader"))

    //val modelFile = config.getString("intel.taproot.scoring-engine.scoring.model")
    val service = initializeScoringServiceDependencies(modelLoader.asInstanceOf[ModelLoader], ModelBytesFileName)

    createActorSystemAndBindToHttp(service)
  }

  private def initializeScoringServiceDependencies(modelLoader: ModelLoader, modelFile: String): ScoringService = {
    val source = scala.io.Source.fromFile(modelFile)
    val byteArray = source.map(_.toByte).toArray
    source.close()
    val model = modelLoader.load(byteArray)
    val modelName = modelFile.substring(modelFile.lastIndexOf("/") + 1)
    new ScoringService(model, modelName)
  }

  private def readModelInfo(): Unit = {

    info("archive tar = " + config.getString("intel.taproot.scoring-engine.archive-tar"))
    val myTarFile: TarArchiveInputStream = new TarArchiveInputStream(new FileInputStream(new File(config.getString("intel.taproot.scoring-engine.archive-tar"))));
    var entry: TarArchiveEntry = null;
    entry = myTarFile.getNextTarEntry()
    while (entry != null) {
      // Get the name of the file
      val individualFile: String = entry.getName();
      // Get Size of the file and create a byte array for the size
      val content: Array[Byte] = new Array[Byte](entry.getSize.toInt);
      /* Some SOP statements to check progress */
      info("File Name in TAR File is: " + individualFile);
      info("Size of the File is: " + entry.getSize());
      info("Byte Array length: " + content.length);
      // Read file from the archive into byte array
      myTarFile.read(content, 0, content.length);
      //Define OutputStream for writing the file
      val outputFile = new FileOutputStream(new File(individualFile));
      IOUtils.write(content, outputFile);
      outputFile.close();
      if (individualFile.contains(".jar")) {
        archiveName = individualFile.substring(0, individualFile.indexOf(".jar"))
        info("Archive name is  " + archiveName);
      }
      else if (individualFile.contains("modelname")) {
        modelName = new String(content)
        info("Model name is  " + modelName);
      }
      else {
        ModelBytesFileName = individualFile
        info("Model file name is  " + ModelBytesFileName);
      }
      entry = myTarFile.getNextTarEntry()
    }
    myTarFile.close();
  }
  /**
   * We need an ActorSystem to host our application in and to bind it to an HTTP port
   */
  private def createActorSystemAndBindToHttp(scoringService: ScoringService): Unit = {
    // create the system
    implicit val system = ActorSystem("taprootanalytics-scoring")
    implicit val timeout = Timeout(5.seconds)
    val service = system.actorOf(Props(new ScoringServiceActor(scoringService)), "scoring-service")
    // Bind the Spray Actor to an HTTP Port
    // start a new HTTP server with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = config.getString("intel.taproot.analytics.scoring.host"), port = config.getInt("intel.taproot.analytics.scoring.port"))
  }

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = {
    throw new Exception("API server provides no components at this time")
  }
}
