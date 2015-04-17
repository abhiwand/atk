//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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
import com.intel.intelanalytics.engine.spark.frame.{ FrameFileStorage, SparkFrameStorage }
import com.intel.intelanalytics.repository.{ GarbageCollectableRepository, MetaStore, MetaStoreComponent }
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import scala.util.Try

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
  def garbageCollectEntities(gcAgeToDeleteData: Long = SparkEngineConfig.gcAgeToDeleteData,
                             gcAgeToDeleteMetaData: Long = SparkEngineConfig.gcAgeToDeleteMetaData): Unit = {
    this.synchronized {
      metaStore.withSession("gc.garbagecollector") {
        implicit session =>
          try {
            if (gcRepo.getCurrentExecutions().length == 0) {
              info("Execute Garbage Collector")
              val gc: GarbageCollection = gcRepo.insert(new GarbageCollectionTemplate(getHostName(), getProcessId(), new DateTime)).get
              garbageCollectFrames(gc, gcAgeToDeleteData, gcAgeToDeleteMetaData)
              garbageCollectGraphs(gc, gcAgeToDeleteData, gcAgeToDeleteMetaData)
              garbageCollectModels(gc, gcAgeToDeleteData, gcAgeToDeleteMetaData)
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
  def garbageCollectFrames(gc: GarbageCollection, gcAgeToDeleteData: Long, gcAgeToDeleteMetaData: Long)(implicit session: metaStore.Session): Unit = {
    //get weakly live records that are old
    metaStore.frameRepo.listReadyForDeletion(gcAgeToDeleteData).foreach(frame => {
      deleteFrameData(gc, frame)
    })
    //get dead records that are old
    metaStore.frameRepo.listReadyForMetaDataDeletion(gcAgeToDeleteMetaData).foreach(frame => {
      val description = s"Marking MetaData as Deleted for DataFrame ID: ${frame.id} Name: ${frame.name}"
      try {
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.frameRepo.updateMetaDataDeleted(frame)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(s"Exception when: $description", exception = e)
      }
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
  def garbageCollectGraphs(gc: GarbageCollection, gcAgeToDeleteData: Long, gcAgeToDeleteMetaData: Long)(implicit session: metaStore.Session): Unit = {
    metaStore.graphRepo.listReadyForDeletion(SparkEngineConfig.gcAgeToDeleteData).foreach(graph => {
      val description = s"Deleting Data for Graph ID: ${graph.id} Name: ${graph.name}"
      try {
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.frameRepo.lookupByGraphId(graph.id).foreach(frame => deleteFrameData(gc, frame))
        metaStore.graphRepo.updateDataDeleted(graph)
        if (graph.isTitan) {
          graphBackendStorage.deleteUnderlyingTable(graph.storage, quiet = true)(invocation = new BackendInvocation())
        }
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(s"Exception when: $description", exception = e)
      }
    })

    metaStore.graphRepo.listReadyForMetaDataDeletion(SparkEngineConfig.gcAgeToDeleteMetaData).foreach(graph => {
      val description = s"Deleting MetaData for Graph ID: ${graph.id} Name: ${graph.name}"
      try {
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.graphRepo.updateMetaDataDeleted(graph)
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
  def garbageCollectModels(gc: GarbageCollection, gcAgeToDeleteData: Long, gcAgeToDeleteMetaData: Long)(implicit session: metaStore.Session): Unit = {
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

    metaStore.modelRepo.listReadyForMetaDataDeletion(SparkEngineConfig.gcAgeToDeleteMetaData).foreach(model => {
      val description = s"Deleting MetaData for Model ID: ${model.id} Name: ${model.name}"
      try {
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.modelRepo.updateMetaDataDeleted(model)
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
   * @param gcAgeToDeleteMetaData
   */
  def singleTimeExecution(gcAgeToDeleteData: Long, gcAgeToDeleteMetaData: Long): Unit = {
    require(garbageCollector != null, "GarbageCollector has not been initialized. Problem during RestServer initialization")
    require(gcAgeToDeleteData < gcAgeToDeleteMetaData, "MetaData Deletion age may not be less than Data Deletion age.")
    garbageCollector.garbageCollectEntities(gcAgeToDeleteData, gcAgeToDeleteMetaData)
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
