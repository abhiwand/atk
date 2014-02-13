package com.intel.graph;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
public class TestGraphExporter {

    @Test
    public void testGetResultFolder_no_job_step() {
        boolean isExceptionRaised = false;
        FileStatus[] fileStatuses = new FileStatus[]{};
        try {
            GraphExporter.getResultFolder(fileStatuses);
        } catch(Exception e) {
            System.out.println(e.toString());
            isExceptionRaised = true;
        }
        assertEquals(isExceptionRaised, true);
    }

    @Test
    public void testGetResultFolder_single_job_step() {
        FileStatus status1 = mock(FileStatus.class);
        Path path1 = mock(Path.class);
        when(path1.getName()).thenReturn("job-0");
        when(status1.getPath()).thenReturn(path1);
        FileStatus[] fileStatuses = new FileStatus[]{status1};
        Path path = GraphExporter.getResultFolder(fileStatuses);
        assertEquals(path.getName(), "job-0");
    }

    @Test
    public void testGetResultFolder_two_job_step() {
        FileStatus status0 = mock(FileStatus.class);
        Path path0 = mock(Path.class);
        when(path0.getName()).thenReturn("job-0");
        when(status0.getPath()).thenReturn(path0);

        FileStatus status1 = mock(FileStatus.class);
        Path path1 = mock(Path.class);
        when(path1.getName()).thenReturn("job-1");
        when(status1.getPath()).thenReturn(path1);

        FileStatus[] fileStatuses = new FileStatus[]{status0, status1};
        Path path = GraphExporter.getResultFolder(fileStatuses);
        assertEquals(path.getName(), "job-1");
    }

    @Test
    public void testGraphElementFactory_getEdge() {
        IGraphElementFactory factory = new TitanFaunusGraphElementFactory();
        IGraphElement element = factory.makeElement("[e[64007973][4-edge->6000264], {etl-cf:edge_type=tr, etl-cf:weight=4}]\n");
        assertEquals(element.getElementType(), GraphElementType.Edge);
        assertEquals(64007973, element.getId());
        assertEquals("tr", element.getAttributes().get("etl-cf:edge_type"));
        assertEquals("4", element.getAttributes().get("etl-cf:weight"));
        EdgeElement edge = (EdgeElement)element;
        assertEquals(4, edge.getOutVertexId());
        assertEquals(6000264, edge.getInVertexId());
    }


    @Test
    public void testGraphElementFactory_getVertex() {
        IGraphElementFactory factory = new TitanFaunusGraphElementFactory();
        IGraphElement element = factory.makeElement("[v[800316], {_gb_ID=-164, etl-cf:vertex_type=R}]");
        assertEquals(element.getElementType(), GraphElementType.Vertex);
        assertEquals(800316, element.getId());
        assertEquals("-164", element.getAttributes().get("_gb_ID"));
        assertEquals("R", element.getAttributes().get("etl-cf:vertex_type"));
    }

    @Test
    public void testByteArrayStream() throws IOException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        f.write("test1".getBytes());
        f.write(",test2".getBytes());
        f.write(",test3".getBytes());

        assertEquals("test1,test2,test3", f.toString());
    }

    @Test
    public void testGetKeyTypesMapping() throws ParserConfigurationException, SAXException, IOException {
        String schemaXML = "<?xml version=\"1.0\" ?><schema><feature attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"etl-cf:weight\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"_id\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"_gb_ID\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\" for=\"Vertex\"></feature></schema>";
        Reader reader = new StringReader(schemaXML);

        Map<String, String> vertexKeyTypes = new HashMap<String, String>();
        Map<String, String> edgeKeyTypes = new HashMap<String, String>();
        GraphExportReducer.getKeyTypesMapping(reader, vertexKeyTypes, edgeKeyTypes);

        assertEquals(edgeKeyTypes.get("etl-cf:edge_type"), "bytearray");
        assertEquals(edgeKeyTypes.get("etl-cf:weight"), "bytearray");
        assertEquals(edgeKeyTypes.size(), 2);

        assertEquals(vertexKeyTypes.get("_id"), "bytearray");
        assertEquals(vertexKeyTypes.get("_gb_ID"), "bytearray");
        assertEquals(vertexKeyTypes.get("etl-cf:vertex_type"), "bytearray");
        assertEquals(vertexKeyTypes.size(), 3);
    }

    @Test
    public void testGetStatementListFromXMLString_0_statments() throws IOException, SAXException, ParserConfigurationException {
        String xml = "<query></query>";
        List<String> statements = GraphExporter.getStatementListFromXMLString(xml);
        assertEquals(0, statements.size());
    }

    @Test
    public void testGetStatementListFromXMLString_2_statments() throws IOException, SAXException, ParserConfigurationException {
        String xml = "<query><statement>g.V('_gb_ID','11').out.map</statement><statement>g.V('_gb_ID','11').outE.transform('{[it,it.map()]}')</statement></query>";
        List<String> statements = GraphExporter.getStatementListFromXMLString(xml);
        assertEquals(2, statements.size());
        assertEquals("g.V('_gb_ID','11').out.map", statements.get(0));
        assertEquals("g.V('_gb_ID','11').outE.transform('{[it,it.map()]}')", statements.get(1));
    }

    @Test
    public void testGraphMLGeneration() throws XMLStreamException, ParserConfigurationException, SAXException, IOException {
        GraphExportReducer reducer = new GraphExportReducer();
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        XMLOutputFactory xmlInputFactory = XMLOutputFactory.newInstance();
        xmlInputFactory.setProperty("escapeCharacters", false);
        XMLStreamWriter writer = xmlInputFactory.createXMLStreamWriter(f);


        String schemaXML = "<?xml version=\"1.0\" ?><schema><feature attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"etl-cf:weight\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"_id\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"_gb_ID\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\" for=\"Vertex\"></feature></schema>";
        Reader reader = new StringReader(schemaXML);

        Map<String, String> vertexKeyTypes = new HashMap<String, String>();
        Map<String, String> edgeKeyTypes = new HashMap<String, String>();
        GraphExportReducer.getKeyTypesMapping(reader, vertexKeyTypes, edgeKeyTypes);

        reducer.writeGraphMLHeaderSection(writer, vertexKeyTypes, edgeKeyTypes);
        reducer.writeElementData(writer, "<edge id=\"62018421\" source=\"4\" target=\"5600276\" label=\"label\"><data key=\"etl-cf:edge_type\">tr</data><data key=\"etl-cf:weight\">3</data></edge>");
        reducer.writeElementData(writer, "<node id=\"4000284\"><data key=\"_id\">4000284</data><data key=\"_gb_ID\">-304</data><data key=\"etl-cf:vertex_type\">R</data></node>");
        reducer.writeGraphMLEndSection(writer);
        String result = f.toString();
        String expected = "<?xml version=\"1.0\" ?><graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd\"><key id=\"_id\" for=\"node\" attr.name=\"_id\" attr.type=\"bytearray\"></key><key id=\"_gb_ID\" for=\"node\" attr.name=\"_gb_ID\" attr.type=\"bytearray\"></key><key id=\"etl-cf:vertex_type\" for=\"node\" attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\"></key><key id=\"etl-cf:edge_type\" for=\"edge\" attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\"></key><key id=\"etl-cf:weight\" for=\"edge\" attr.name=\"etl-cf:weight\" attr.type=\"bytearray\"></key><graph id=\"G\" edgedefault=\"directed\">" +
                "<edge id=\"62018421\" source=\"4\" target=\"5600276\" label=\"label\"><data key=\"etl-cf:edge_type\">tr</data><data key=\"etl-cf:weight\">3</data></edge>\n" +
                "<node id=\"4000284\"><data key=\"_id\">4000284</data><data key=\"_gb_ID\">-304</data><data key=\"etl-cf:vertex_type\">R</data></node>\n" +
                "</graph></graphml>";
        assertEquals(expected, result);
    }
}
