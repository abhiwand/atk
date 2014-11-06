package com.intel.intelanalytics.domain.graph

/**
 * Class containing the list of Element ID names corresponding to GraphElements and the name of their id column
 */
case class ElementIDNames(elementIDNames: List[ElementIDName]) {
  require(elementIDNames != null, "element id names cannot be null")
}

/**
 * Name Value pairing of a graph element and its corresponding id column name
 * @param elementName label of graph element
 * @param idColumnName column name of unique identifier.
 */
case class ElementIDName(elementName: String, idColumnName: String) {
  require(!elementName.isEmpty, "element name is required")
  require(!idColumnName.isEmpty, "idColumnName is required")
}
