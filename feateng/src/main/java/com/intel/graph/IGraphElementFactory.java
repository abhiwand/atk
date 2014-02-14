package com.intel.graph;

/**
 * Factory for generating graph elements
 */
public interface IGraphElementFactory {
    /**
     * Create graph element from text content
     * @param text: Text representation source for the graph element
     * @return The graph element
     */
    IGraphElement makeElement(String text);
}
