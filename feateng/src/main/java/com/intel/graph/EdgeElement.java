package com.intel.graph;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represent an edge object in the graph
 */
public class EdgeElement implements IGraphElement {

    Map<String, Object> attributes = new HashMap<String, Object>();
    long id = 0;

    long inVertexId = 0;
    long outVertexId = 0;

    public EdgeElement(long id) {
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public GraphElementType getElementType() {
        return GraphElementType.Edge;
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
        writer.writeStartElement(GraphMLTokens.EDGE);
        writer.writeAttribute(GraphMLTokens.ID, String.valueOf(getId()));
        writer.writeAttribute(GraphMLTokens.SOURCE, String.valueOf(getOutVertexId()));
        writer.writeAttribute(GraphMLTokens.TARGET, String.valueOf(getInVertexId()));

        writer.writeAttribute(GraphMLTokens.LABEL, "label");
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

    /**
     * get incoming vertex's id
     * @return incoming vertex's id
     */
    public long getInVertexId() {
        return inVertexId;
    }

    /**
     * set incoming vertex' id
     * @param inVertexId: incoming vertex's id
     */
    public void setInVertexId(long inVertexId) {
        this.inVertexId = inVertexId;
    }

    /**
     * return outgoing vertex's id
     * @return outgoing vertex's id
     */
    public long getOutVertexId() {
        return outVertexId;
    }

    /**
     * set outgoing vertex's id
     * @param outVertexId: outgoing vertex's id
     */
    public void setOutVertexId(long outVertexId) {
        this.outVertexId = outVertexId;
    }
}
