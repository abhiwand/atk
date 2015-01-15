package com.intel.intelanalytics.engine.gc

import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.util.concurrent.{ Executors, TimeUnit, ScheduledFuture }

import com.intel.event.EventLogging
import com.intel.intelanalytics.domain.gc.{ GarbageCollectionEntryTemplate, GarbageCollectionEntry, GarbageCollectionTemplate, GarbageCollection }
import com.intel.intelanalytics.engine.GraphBackendStorage
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.{ FrameFileStorage, SparkFrameStorage }
import com.intel.intelanalytics.repository.{ GarbageCollectableRepository, MetaStore, MetaStoreComponent }
import com.typesafe.config.ConfigFactory
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

  override def run(): Unit = {
    this.synchronized {
      metaStore.withSession("garbagecollector") {
        implicit session =>
          try {
            if (gcRepo.getCurrentExecutions().length == 0) {
              info("Execute Garbage Collector")

              val gc: GarbageCollection = gcRepo.insert(new GarbageCollectionTemplate(getHostName(), getProcessId(), new DateTime)).get

              garbageCollectEntities(gc)
              gcRepo.updateEndTime(gc)
            }
            else {
              info("Garbage Collector currently executing in another process.")
            }
          }
          catch {
            case e: Exception => error(e.toString)
          }
      }
    }
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
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectEntities(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    garbageCollectFrames(gc)
    garbageCollectGraphs(gc)
    garbageCollectModels(gc)
  }

  /**
   * garbage collect frames delete saved files if the frame is too old and mark object as deleted if it has not been
   * called in over a year
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectFrames(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    //get weakly live records that are old
    metaStore.frameRepo.listReadyForDeletion(SparkEngineConfig.gcAgeToDeleteData).foreach(frame => {
      try {
        val description = s"Deleting Data for DataFrame ID: ${frame.id} Name: ${frame.name}"
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        frameStorage.delete(frame)
        metaStore.frameRepo.updateDataDeleted(frame)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(e.toString)
      }
    })
    //get dead records that are old
    metaStore.frameRepo.listReadyForMetaDataDeletion(SparkEngineConfig.gcAgeToDeleteMetaData).foreach(frame => {
      try {
        val description = s"Marking MetaData as Deleted for DataFrame ID: ${frame.id} Name: ${frame.name}"
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.frameRepo.updateMetaDataDeleted(frame)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(e.toString)
      }
    })
  }

  /**
   * garbage collect graphs delete underlying frame rdds for a seamless graph and mark as deleted if not referenced
   * in over a year
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectGraphs(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    metaStore.graphRepo.listReadyForDeletion(SparkEngineConfig.gcAgeToDeleteData).foreach(graph => {
      try {
        val description = s"Deleting Data for Graph ID: ${graph.id} Name: ${graph.name}"
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.graphRepo.updateDataDeleted(graph)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(e.toString)
      }
    })

    metaStore.graphRepo.listReadyForMetaDataDeletion(SparkEngineConfig.gcAgeToDeleteMetaData).foreach(graph => {
      try {
        val description = s"Deleting MetaData for Graph ID: ${graph.id} Name: ${graph.name}"
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.graphRepo.updateMetaDataDeleted(graph)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(e.toString)
      }
    })
  }

  /**
   * garbage collect models and mark as deleted if not referenced in over a year
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def garbageCollectModels(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    metaStore.modelRepo.listReadyForDeletion(SparkEngineConfig.gcAgeToDeleteData).foreach(model => {
      try {
        val description = s"Deleting Data for Model ID: ${model.id} Name: ${model.name}"
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.modelRepo.updateDataDeleted(model)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(e.toString)
      }
    })

    metaStore.modelRepo.listReadyForMetaDataDeletion(SparkEngineConfig.gcAgeToDeleteMetaData).foreach(model => {
      try {
        val description = s"Deleting MetaData for Model ID: ${model.id} Name: ${model.name}"
        val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
          new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
        info(description)
        metaStore.modelRepo.updateMetaDataDeleted(model)
        gcEntryRepo.updateEndTime(gcEntry)
      }
      catch {
        case e: Exception => error(e.toString)
      }
    })
  }
}

object GarbageCollector {
  var gcScheduler: ScheduledFuture[_] = null

  /**
   * start the garbage collector thread
   * @param metaStore database store
   * @param frameStorage storage class for accessing frame storage
   * @param graphBackendStorage storage class for accessing graph backend storage
   */
  def startup(metaStore: MetaStore, frameStorage: FrameFileStorage, graphBackendStorage: GraphBackendStorage): Unit = {
    this.synchronized {
      if (gcScheduler == null) {
        gcScheduler = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
          new GarbageCollector(metaStore, frameStorage, graphBackendStorage), 0, SparkEngineConfig.gcInterval, TimeUnit.MILLISECONDS)
      }
    }
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

