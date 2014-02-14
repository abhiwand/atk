package com.intel.graph;


import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Map;

/**
 *  Interface for graph element
 */
public interface IGraphElement {

    /**
     *
     * @return  the unique identifier for element in the graph
     */
    long getId();

    /**
     *
     * @return type of this element
     */
    GraphElementType getElementType();

    /**
     *
     * @return mapping of attribute names and values
     */
    Map<String, Object> getAttributes();

    /**
     * Set attributes to this element
     * @param attributes
     */
    void setAttributes(Map<String, Object> attributes);

    /**
     * Write this element's content to xml stream
     * @param writer: stream writer to xml output
     * @throws XMLStreamException
     */
    void writeToXML(XMLStreamWriter writer) throws XMLStreamException;
}
