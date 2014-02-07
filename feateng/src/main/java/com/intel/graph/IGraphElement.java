package com.intel.graph;


import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Map;

public interface IGraphElement {

    long getId();
    GraphElementType getElementType();
    Map<String, Object> getAttributes();
    void setAttributes(Map<String, Object> attributes);
    void writeToXML(XMLStreamWriter writer) throws XMLStreamException;
}
