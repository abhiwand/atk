package com.intel.graphbuilder.elements

/**
 * This wrapper maps a gbId to a Physical ID
 * <p>
 * This is to get around the fact that Titan won't allow you to specify physical ids.
 * It might be useful for other vendors too though.
 * </p>
 *
 * @param gbId the unique property used by Graph Builder
 * @param physicalId the ID used by the underlying graph storage
 */
case class GbIdToPhysicalId(gbId: Property, physicalId: AnyRef) {

}
