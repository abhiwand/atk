import org.apache.commons.configuration.BaseConfiguration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Direction;

public class graphTest {
    public static void main(String[] args) {
        String conf = '/home/hadoop/IntelAnalytics/titan/conf/titan-hbase-es.properties';
        Graph g = TitanFactory.open(conf);
        GraphOfTheGodsFactory.load(g);
        println("Loaded example graph from " + conf);

        for (Vertex v in g.V() ) {
            println v;
        }
        for (Edge e in g.E() ) {
            println e;
        }
        println g.V().map();
        println g.E().map();
    }
}
