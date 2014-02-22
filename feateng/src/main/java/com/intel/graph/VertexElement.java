package com.intel.graph;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represent a vertex element in the class.
 */
public class VertexElement implements IGraphElement{

    Map<String, Object> attributes = new HashMap<String, Object>();
    long id = 0;

    public VertexElement(long id) {
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public GraphElementType getElementType() {
        return GraphElementType.Vertex;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    @Override
    public void writeToXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement(GraphMLTokens.NODE);
        writer.writeAttribute(GraphMLTokens.ID, String.valueOf(getId()));
        Collection<String> keys = getAttributes().keySet();

        for (String attributeKey : keys) {
            writer.writeStartElement(GraphMLTokens.DATA);
            writer.writeAttribute(GraphMLTokens.KEY, attributeKey);
            Object attributeValue = getAttributes().get(attributeKey);
            if (null != attributeValue) {
                writer.writeCharacters(attributeValue.toString());
            }
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }

}
