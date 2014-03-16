import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Vertex
import org.apache.commons.configuration.BaseConfiguration


split(args)

def split(args){
    String[] inputs = args[0].split("\\::")
    conf = new BaseConfiguration()
    conf.setProperty("storage.tablename", inputs[0])
    conf.setProperty("storage.backend", inputs[1])
    conf.setProperty("storage.hostname", inputs[2])
    conf.setProperty("storage.port", inputs[3])
    conf.setProperty("storage.connection_timeout", inputs[4])
    test_fold_id = inputs[5].toInteger()
    fold_id_property_key = inputs[6]
    split_name = inputs[7].split("\\;")
    split_property_key = inputs[8]
    type = inputs[9]

    if (split_name.size() != 2) {
        throw new IllegalArgumentException("The number of split_name should be 2!")
    }

    g = TitanFactory.open(conf)
    if (type == "EDGE"){
        for(Edge e : g.E){
            fold_id = e.getProperty(fold_id_property_key).toDouble()
            split_label = (fold_id >= test_fold_id && fold_id < (test_fold_id + 1)) ? split_name[0] : split_name[1]
            e.setProperty(split_property_key, split_label)
        }
    } else if (type == "VERTEX") {
        for(Vertex v : g.V){
            fold_id = v.getProperty(fold_id_property_key).toDouble()
            split_label = (fold_id >= test_fold_id && fold_id < (test_fold_id + 1)) ? split_name[0] : split_name[1]
            v.setProperty(split_property_key, split_label)
        }
    } else {
        throw new IllegalArgumentException("The number of split_name should be 2!")
    }


    println 'complete execution'
}
