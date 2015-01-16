package com.intel.intelanalytics.domain.gc

import org.joda.time.DateTime

/**
 * Arguments needed for creating a new instance of a GarbageCollectionEntry
 * @param garbageCollectionId unique Id of the garbage collection
 * @param description description of the entity being garbage collected
 * @param startTime time deletion starts
 */
case class GarbageCollectionEntryTemplate(garbageCollectionId: Long,
                                          description: String,
                                          startTime: DateTime)
