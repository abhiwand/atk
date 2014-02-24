package com.intel.graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Provide methods to construct graphml
 */
public class GraphMLWriter {
    /**
     * Write the ending tags for the graphml file
     * @param writer: stream writer to xml output
     */
    public static void writeGraphMLEndSection(XMLStreamWriter writer) {
        try {
            writer.writeEndElement(); // graph
            writer.writeEndElement(); // graphml
            writer.writeEndDocument();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to write closing tags", e);
        }
    }

    /**
     * Write the header section for the graphml file
     *
     * @param writer : stream writer to xml output
     * @param vertexKeyTypes : mapping of vetex's property name and property type
     * @param edgeKeyTypes : mapping of edge's property name and property type
     * @throws javax.xml.stream.XMLStreamException
     */
    public static void writeGraphMLHeaderSection(XMLStreamWriter writer, Map<String, String> vertexKeyTypes, Map<String, String> edgeKeyTypes) throws XMLStreamException {
        writer.writeStartDocument();
        writer.writeStartElement(GraphMLTokens.GRAPHML);
        writer.writeAttribute(GraphMLTokens.XMLNS, GraphMLTokens.GRAPHML_XMLNS);

        //XML Schema instance namespace definition (xsi)
        writer.writeAttribute(XMLConstants.XMLNS_ATTRIBUTE + ":" + GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG,
                XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
        //XML Schema location
        writer.writeAttribute(GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG + ":" + GraphMLTokens.XML_SCHEMA_LOCATION_ATTRIBUTE,
                GraphMLTokens.GRAPHML_XMLNS + " " + GraphMLTokens.DEFAULT_GRAPHML_SCHEMA_LOCATION);

        writeSchemaInfo(writer, vertexKeyTypes, GraphMLTokens.NODE);
        writeSchemaInfo(writer, edgeKeyTypes, GraphMLTokens.EDGE);

        writer.writeStartElement(GraphMLTokens.GRAPH);
        writer.writeAttribute(GraphMLTokens.ID, GraphMLTokens.G);
        writer.writeAttribute(GraphMLTokens.EDGEDEFAULT, GraphMLTokens.DIRECTED);
    }

    /**
     * Write schema to schema section for the graphml file
     * @param writer: stream writer to xml output
     * @param vertexKeyTypes: mapping of vetex's property name and property type
     * @param node: The string representation for the type of the element
     * @throws javax.xml.stream.XMLStreamException
     */
    private static void writeSchemaInfo(XMLStreamWriter writer, Map<String, String> vertexKeyTypes, String node) throws XMLStreamException {
        Set<String> keySet = vertexKeyTypes.keySet();
        for (String key : keySet) {
            writer.writeStartElement(GraphMLTokens.KEY);
            writer.writeAttribute(GraphMLTokens.ID, key);
            writer.writeAttribute(GraphMLTokens.FOR, node);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, vertexKeyTypes.get(key));
            writer.writeEndElement();
        }
    }

    /**
     * Write the element node data which is from mapper to the graphml file
     * @param writer: stream writer to xml output
     * @param value: The content of the element node data. eg: <node id="800380"><data key="_id">800380</data><data key="_gb_ID">-395</data><data key="etl-cf:vertex_type">R</data></node>
     */
    public static void writeElementData(XMLStreamWriter writer, String value) {
        try {
            writer.writeCharacters(value);
            writer.writeCharacters("\n");
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to write graph element data", e);
        }
    }

    /**
     * Delete any existing file with the same name in the same path and create a new file
     * @param fileName: The file to be created
     * @param fs: FileSystem object for the current file system
     * @return FSDataOutputStream object
     * @throws IOException
     */
    static FSDataOutputStream createFile(String fileName, FileSystem fs) throws IOException {
        Path outputFilePath = new Path(fileName);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return fs.create(outputFilePath, true);
    }
}
