package com.intel.graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphExportReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private FSDataOutputStream output;
    XMLStreamWriter writer = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String fileName = conf.get("file");

        Path path = new Path(fileName);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        output = fs.create(path, true);

        Map<String, String> vertexKeyTypes = new HashMap<String, String>();
        Map<String, String> edgeKeyTypes = new HashMap<String, String>();

        vertexKeyTypes.put("_id","long");
        vertexKeyTypes.put("_gb_ID","long");
        vertexKeyTypes.put("etl-cf:vertex_type","chararray");

        edgeKeyTypes.put("etl-cf:edge_type","chararray");
        edgeKeyTypes.put("etl-cf:weight","long");

        final XMLOutputFactory inputFactory = XMLOutputFactory.newInstance();
        inputFactory.setProperty("escapeCharacters", false);

        try {
            writer = inputFactory.createXMLStreamWriter(output, "UTF8");
            writer.writeStartDocument();
            writer.writeStartElement(GraphMLTokens.GRAPHML);
            writer.writeAttribute(GraphMLTokens.XMLNS, GraphMLTokens.GRAPHML_XMLNS);
            //XML Schema instance namespace definition (xsi)
            writer.writeAttribute(XMLConstants.XMLNS_ATTRIBUTE + ":" + GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG,
                    XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
            //XML Schema location
            writer.writeAttribute(GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG + ":" + GraphMLTokens.XML_SCHEMA_LOCATION_ATTRIBUTE,
                    GraphMLTokens.GRAPHML_XMLNS + " " + GraphMLTokens.DEFAULT_GRAPHML_SCHEMA_LOCATION);

            Set<String> keySet = vertexKeyTypes.keySet();
            for (String key : keySet) {
                writer.writeStartElement(GraphMLTokens.KEY);
                writer.writeAttribute(GraphMLTokens.ID, key);
                writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.NODE);
                writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
                writer.writeAttribute(GraphMLTokens.ATTR_TYPE, vertexKeyTypes.get(key));
                writer.writeEndElement();
            }
            keySet = edgeKeyTypes.keySet();
            for (String key : keySet) {
                writer.writeStartElement(GraphMLTokens.KEY);
                writer.writeAttribute(GraphMLTokens.ID, key);
                writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.EDGE);
                writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
                writer.writeAttribute(GraphMLTokens.ATTR_TYPE, edgeKeyTypes.get(key));
                writer.writeEndElement();
            }

            writer.writeStartElement(GraphMLTokens.GRAPH);
            writer.writeAttribute(GraphMLTokens.ID, GraphMLTokens.G);
            writer.writeAttribute(GraphMLTokens.EDGEDEFAULT, GraphMLTokens.DIRECTED);
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to write header");
        }
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) {

        for(Text value: values){
            try {
                writer.writeCharacters(value.toString());
                writer.writeCharacters("\n");
            } catch (XMLStreamException e) {
                throw new RuntimeException("Failed to write graph element data");
            }
        }

    }

    @Override
    protected void cleanup(Context context) {
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
