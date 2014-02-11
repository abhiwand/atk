package com.intel.graph;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        String schemaXML = "<schema><feature name=\"etl-cf:edge_type\" type=\"chararray\" /><feature name=\"etl-cf:weight\" type=\"long\" /></schema>";
        Map<String, String> mapping = GraphExportReducer.getKeyTypesMapping(schemaXML);
        assertTrue(mapping.containsKey("etl-cf:edge_type"));
        assertTrue(mapping.containsKey("etl-cf:weight"));
        assertEquals("chararray", mapping.get("etl-cf:edge_type"));
        assertEquals("long", mapping.get("etl-cf:weight"));
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
}
