package com.intel.intelanalytics.service.v1.viewmodels

/**
 * Case Class for listing the number of pages taht a query contains
 * @param id id of page
 * @param url url to request page
 */
case class GetQueryPages(id: Long, url: String)
