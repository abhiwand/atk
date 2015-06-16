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

package com.intel.intelanalytics.engine.spark.gc

import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.util.concurrent.{ Executors, TimeUnit, ScheduledFuture }

import com.intel.event.EventLogging
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.gc.{ GarbageCollectionEntryTemplate, GarbageCollectionEntry, GarbageCollectionTemplate, GarbageCollection }
import com.intel.intelanalytics.engine.GraphBackendStorage
import com.intel.intelanalytics.engine.plugin.BackendInvocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.{ FrameFileStorage }
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext
import com.intel.intelanalytics.repository.{ MetaStore }
import org.joda.time.DateTime

/**
 * Runnable Thread that executes garbage collection of unused entities.
 * @param metaStore database store
 * @param frameStorage storage class for accessing frame storage
 * @param graphBackendStorage storage class for accessing graph backend storage
 */
class GarbageCollector(val metaStore: MetaStore, val frameStorage: FrameFileStorage, graphBackendStorage: GraphBackendStorage) extends Runnable with EventLogging {

  val start = new DateTime()
  val gcRepo = metaStore.gcRepo
  val gcEntryRepo = metaStore.gcEntryRepo

  /**
   * Execute Garbage Collection as a Runnable
   */
  override def run(): Unit = {
    garbageCollectEntities()
  }

  /**
   * @return get host name of computer executing this process
   */
  def getHostName(): String =
    InetAddress.getLocalHost.getHostName

  /**
   * @return get the process id of the executing process
   */
  def getProcessId(): Long =
    ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toLong

  /**
   * garbage collect all entities
   */
  def garbageCollectEntities(gcAgeToDeleteData: Long = SparkEngineConfig.gcAgeToDeleteData): Unit = {
    this.synchronized {
      metaStore.withSession("gc.garbagecollector") {
        implicit session =>
          try {
            if (gcRepo.getCurrentExecutions().length == 0) {
              info("Execute Garbage Collector")
              val gc: GarbageCollection = gcRepo.insert(new GarbageCollectionTemplate(getHostName(), getProcessId(), new DateTime)).get
              garbageCollectFrames(gc, gcAgeToDeleteData)
              garbageCollectGraphs(gc, gcAgeToDeleteData)
              garbageCollectModels(gc, gcAgeToDeleteData)
              gcRepo.updateEndTime(gc)
            }
            else {
              info("Garbage Collector currently executing in another process.")
            }
          }
          catch {
            case e: Exception => error("Exception Thrown during Garbage Collection", exception = e)
          }
          finally {

          }
      }
    }

  }

  /**
   * garbage collect frames delete saved files if the frame is too old and mark object as deleted if it has not been
   * called in over a year
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectFrames(gc: GarbageCollection, gcAgeToDeleteData: Long)(implicit session: metaStore.Session): Unit = {
    //get weakly live records that are old
    metaStore.frameRepo.listReadyForDeletion(gcAgeToDeleteData).foreach(frame => {
      deleteFrameData(gc, frame)
    })
  }

  /**
   * Method deletes data associated with a frame and places an entry into the GarbageCollectionEntry table
   * @param gc garbage collection database entry
   * @param frame frame to be deleted
   */
  def deleteFrameData(gc: GarbageCollection, frame: FrameEntity)(implicit sesion: metaStore.Session): Unit = {
    val description = s"Deleting Data for DataFrame ID: ${frame.id} Name: ${frame.name}"
    try {
      val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
        new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
      info(description)
      frameStorage.delete(frame)
      metaStore.frameRepo.updateDataDeleted(frame)
      gcEntryRepo.updateEndTime(gcEntry)
    }
    catch {
      case e: Exception => error(s"Exception when: $description", exception = e)
    }
  }

  /**
   * garbage collect graphs delete underlying frame rdds for a seamless graph and mark as deleted if not referenced
   * in over a year
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectGraphs(gc: GarbageCollection, gcAgeToDeleteData: Long)(implicit session: metaStore.Session): Unit = {
    metaStore.graphRepo.listReadyForDeletion(SparkEngineConfig.gcAgeToDeleteData).foreach(graph => {
      val description = s"Deleting Data for Graph ID: ${graph.id} Name: ${graph.name}"
      try {
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.frameRepo.lookupByGraphId(graph.id).foreach(frame => deleteFrameData(gc, frame))
        metaStore.graphRepo.updateDataDeleted(graph)
        if (graph.isTitan) {
          graphBackendStorage.deleteUnderlyingTable(graph.storage, quiet = true)(invocation = new BackendInvocation(EngineExecutionContext.global))
        }
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(s"Exception when: $description", exception = e)
      }
    })
  }

  /**
   * garbage collect models and mark as deleted if not referenced in over a year
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectModels(gc: GarbageCollection, gcAgeToDeleteData: Long)(implicit session: metaStore.Session): Unit = {
    metaStore.modelRepo.listReadyForDeletion(SparkEngineConfig.gcAgeToDeleteData).foreach(model => {
      val description = s"Deleting Data for Model ID: ${model.id} Name: ${model.name}"
      try {
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.modelRepo.updateDataDeleted(model)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(s"Exception when: $description", exception = e)
      }
    })
  }
}

object GarbageCollector {
  private[this] var gcScheduler: ScheduledFuture[_] = null
  private[this] var garbageCollector: GarbageCollector = null

  /**
   * start the garbage collector thread
   * @param metaStore database store
   * @param frameStorage storage class for accessing frame storage
   * @param graphBackendStorage storage class for accessing graph backend storage
   */
  def startup(metaStore: MetaStore, frameStorage: FrameFileStorage, graphBackendStorage: GraphBackendStorage): Unit = {
    this.synchronized {
      if (garbageCollector == null)
        garbageCollector = new GarbageCollector(metaStore, frameStorage, graphBackendStorage)
      if (gcScheduler == null) {
        gcScheduler = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(garbageCollector, 0, SparkEngineConfig.gcInterval, TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * Execute a garbage collection outside of the regularly scheduled intervals
   * @param gcAgeToDeleteData
   */
  def singleTimeExecution(gcAgeToDeleteData: Long): Unit = {
    require(garbageCollector != null, "GarbageCollector has not been initialized. Problem during RestServer initialization")
    garbageCollector.garbageCollectEntities(gcAgeToDeleteData)
  }

  /**
   * shutdown the garbage collector thread
   */
  def shutdown(): Unit = {
    this.synchronized {
      if (gcScheduler != null)
        gcScheduler.cancel(false)
    }
  }
}
