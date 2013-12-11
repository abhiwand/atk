import org.apache.commons.configuration.BaseConfiguration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Direction;


public class graphTest {
    public static void main(String[] args) {
        BaseConfiguration conf = new BaseConfiguration();
        conf.setProperty("storage.backend", "hbase");
        conf.setProperty("storage.hostname", "master,node01,node02,node03");
        conf.setProperty("storage.port","2181");
        conf.setProperty("storage.tablename", "TitanGods");
        conf.setProperty(".index.search.backend", "elasticsearch");
        conf.setProperty(".index.search.directory", "/mnt/data1/titan/searchindex");
        conf.setProperty(".index.search.client-only", "false");
        conf.setProperty(".index.search.local-mode", "true");

        Graph g = TitanFactory.open(conf);
        g.V().map;
        g.E().map
    }
}
