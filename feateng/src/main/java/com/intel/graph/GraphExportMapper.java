package com.intel.graph;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

public class GraphExportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    IGraphElementFactory factory = null;
    XMLOutputFactory inputFactory = null;
    @Override
    protected void setup(Context context) {
        factory = new TitanFaunusGraphElementFactory();
        inputFactory = XMLOutputFactory.newInstance();
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        try {
            XMLStreamWriter writer = inputFactory.createXMLStreamWriter(f);

            IGraphElement element = factory.makeElement(value.toString());
            if(GraphElementType.Vertex == element.getElementType()) {
                writeVertexElementToXML(writer, element);
            } else if(GraphElementType.Edge == element.getElementType()) {
                writeEdgeElementToXML(writer, element);
            }
            context.write(key,new Text(f.toString()));
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to generate xml node for the element");
        }
    }

    private void writeEdgeElementToXML(XMLStreamWriter writer, IGraphElement element) throws XMLStreamException {
        EdgeElement edge = (EdgeElement) element;
        writer.writeStartElement(GraphMLTokens.EDGE);
        writer.writeAttribute(GraphMLTokens.ID, String.valueOf(edge.getId()));
        writer.writeAttribute(GraphMLTokens.SOURCE, String.valueOf(edge.getOutVertexId()));
        writer.writeAttribute(GraphMLTokens.TARGET, String.valueOf(edge.getInVertexId()));

        writer.writeAttribute(GraphMLTokens.LABEL, "label");
        Collection<String> keys = element.getAttributes().keySet();
        for (String attributeKey : keys) {
            writer.writeStartElement(GraphMLTokens.DATA);
            writer.writeAttribute(GraphMLTokens.KEY, attributeKey);
            Object attributeValue = edge.getAttributes().get(attributeKey);
            if (null != attributeValue) {
                writer.writeCharacters(attributeValue.toString());
            }
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }

    private void writeVertexElementToXML(XMLStreamWriter writer, IGraphElement element) throws XMLStreamException {
        writer.writeStartElement(GraphMLTokens.NODE);
        writer.writeAttribute(GraphMLTokens.ID, String.valueOf(element.getId()));
        Collection<String> keys = element.getAttributes().keySet();

        for (String attributeKey : keys) {
            writer.writeStartElement(GraphMLTokens.DATA);
            writer.writeAttribute(GraphMLTokens.KEY, attributeKey);
            Object attributeValue = element.getAttributes().get(attributeKey);
            if (null != attributeValue) {
                writer.writeCharacters(attributeValue.toString());
            }
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }
}
