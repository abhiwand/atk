package com.intel.graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphExportReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private FSDataOutputStream outputStream;
    XMLStreamWriter writer = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String fileName = conf.get(GraphExporter.FILE);

        Map<String, String> edgeKeyTypes = new HashMap<String, String>();
        Map<String, String> vertexKeyTypes = new HashMap<String, String>();

        Path outputFilePath = new Path(fileName);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        outputStream = fs.create(outputFilePath, true);

        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        outputFactory.setProperty("escapeCharacters", false);

        try {
            FileStatus[] fileStatuses = fs.listStatus(TextOutputFormat.getOutputPath(context));
            for (FileStatus status : fileStatuses) {
                String name = status.getPath().getName();
                if(name.startsWith(GraphExporter.METADATA_FILE_PREFIX)) {
                    // collects the schema info created by mappers
                    getKeyTypesMapping(new InputStreamReader(fs.open(status.getPath())), vertexKeyTypes, edgeKeyTypes);
                }
            }

            writer = outputFactory.createXMLStreamWriter(outputStream, "UTF8");
            writeGraphMLHeaderSection(writer, vertexKeyTypes, edgeKeyTypes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header");
        }
    }

    public void writeGraphMLHeaderSection(XMLStreamWriter writer, Map<String, String> vertexKeyTypes, Map<String, String> edgeKeyTypes) throws XMLStreamException {
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

    public void writeSchemaInfo(XMLStreamWriter writer, Map<String, String> vertexKeyTypes, String node) throws XMLStreamException {
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

    public static void getKeyTypesMapping(Reader reader, Map<String, String> vertexKeyTypes, Map<String, String> edgeKeyTypes) throws IOException, SAXException, ParserConfigurationException {

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(new InputSource(reader));

        NodeList nList = doc.getElementsByTagName(GraphExporter.FEATURE);
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Element nNode = (Element)nList.item(temp);
            String name = nNode.getAttribute(GraphMLTokens.ATTR_NAME);
            String type = nNode.getAttribute(GraphMLTokens.ATTR_TYPE);
            String elementType = nNode.getAttribute(GraphMLTokens.FOR);
            if(GraphElementType.Vertex.toString().equalsIgnoreCase(elementType))
                vertexKeyTypes.put(name, type);
            else
                edgeKeyTypes.put(name, type);
        }
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) {

        for(Text value: values){
            writeElementData(writer, value.toString());
        }
    }

    public void writeElementData(XMLStreamWriter writer, String value) {
        try {
            writer.writeCharacters(value);
            writer.writeCharacters("\n");
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to write graph element data");
        }
    }

    @Override
    protected void cleanup(Context context) {
        writeGraphMLEndSection(writer);
    }

    public void writeGraphMLEndSection(XMLStreamWriter writer) {
        try {
            writer.writeEndElement(); // graph
            writer.writeEndElement(); // graphml
            writer.writeEndDocument();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to write closing tags");
        }
    }
}
