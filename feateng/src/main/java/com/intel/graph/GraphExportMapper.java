package com.intel.graph;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Mapper takes in the graph query output and writes out the xml representation for each vertex and edge for reduce phase.
 */
public class GraphExportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    IGraphElementFactory elementFactory = null;
    XMLOutputFactory xmlInputFactory = null;
    IFileOutputStreamGenerator outputStreamGenerator = new FileOutputStreamGenerator();

    Map<String, GraphElementType> propertyElementTypeMapping = new HashMap<String, GraphElementType>();

    @Override
    protected void setup(Context context) {
        elementFactory = new TitanFaunusGraphElementFactory();
        xmlInputFactory = XMLOutputFactory.newInstance();
    }

    /**
     * Convert a line Faunus query output to a graph element object.
     * Then write the element's data as xml.
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        try {
            XMLStreamWriter writer = xmlInputFactory.createXMLStreamWriter(f);
            IGraphElement element = elementFactory.makeElement(value.toString());
            element.writeToXML(writer);
            context.write(key, new Text(f.toString()));
            writer.close();
            // collects schema info
            collectSchemaInfo(element, propertyElementTypeMapping);
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to generate xml node for the element", e);
        }  finally {
            f.close();
        }
    }

    /**
     * Iterating through the attribute name of the element. Record the attribute names and
     * associate them with the element's type in propertyElementTypeMapping
     * @param element: The graph element
     * @param propertyElementTypeMapping: The mapping between attribute name and the type of element that the attribute is from
     */
    public void collectSchemaInfo(IGraphElement element, Map<String, GraphElementType> propertyElementTypeMapping) {
        Set<String> keySet = element.getAttributes().keySet();
        for(String feature : keySet) {
            propertyElementTypeMapping.put(feature, element.getElementType());
        }
    }

    /**
     * Write the collected schema info to intermediate files.
     * The files will be read from reducer to write schema section in the
     * final graphml file.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        TaskAttemptID id = context.getTaskAttemptID();
        Path path = new Path(new File(TextOutputFormat.getOutputPath(context).toString(), GraphExporter.METADATA_FILE_PREFIX + id.toString()).toString());

        OutputStream output = outputStreamGenerator.getOutputStream(context, path);
        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = outputFactory.createXMLStreamWriter(output, "UTF8");
            writeSchemaToXML(writer, propertyElementTypeMapping);
            writer.close();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to export schema info from mapper", e);
        } finally {
            output.close();
        }
    }


    /**
     * Taking the collected schema information and writing it to xml
     * @param writer: stream writer to xml output
     * @param propertyElementTypeMapping mapping between attribute name and element type
     * @throws XMLStreamException
     */
    public void writeSchemaToXML(XMLStreamWriter writer, Map<String, GraphElementType> propertyElementTypeMapping) throws XMLStreamException {
        writer.writeStartDocument();
        writer.writeStartElement(GraphExporter.SCHEMA);

        for(Map.Entry<String, GraphElementType> e : propertyElementTypeMapping.entrySet()) {
            writer.writeStartElement(GraphExporter.FEATURE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, e.getKey());
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, "string");
            writer.writeAttribute(GraphMLTokens.FOR, e.getValue().toString());
            writer.writeEndElement();
        }

        writer.writeEndElement(); // schema
        writer.writeEndDocument();
        writer.flush();
        writer.close();
    }
}

