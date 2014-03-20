import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import org.apache.commons.configuration.BaseConfiguration


def g

def setup(args){
    String[] inputs = args[0].split("\\::")
    conf = new BaseConfiguration()
    conf.setProperty("storage.tablename", inputs[0])
    conf.setProperty("storage.backend", inputs[1])
    conf.setProperty("storage.hostname", inputs[2])
    conf.setProperty("storage.port", inputs[3])
    conf.setProperty("storage.connection_timeout", inputs[4])
    g = TitanFactory.open(conf)
}

def map(input,args){
    String[] inputs = args[0].split("\\::")
    test_fold_id = inputs[5]
    fold_id_property_key = inputs[6]
    split_name = inputs[7].split("\\;")
    split_property_key = inputs[8]
    type = inputs[9]

    if (split_name.size() != 2) {
        throw new IllegalArgumentException("The number of split_name should be 2!")
    }


    if (type == "EDGE"){
        u = g.e(input.id)
    } else if (type == "VERTEX") {
        u = g.v(input.id)
    } else {
        throw new IllegalArgumentException("The number of split_name should be 2!")
    }
    fold_id = u.getProperty(fold_id_property_key).toDouble()
    split_label = (fold_id >= test_fold_id && fold_id < (test_fold_id + 1)) ? split_name[0] : split_name[1]
    u.setProperty(split_property_key, split_label)
}



def cleanup(args){
    g.shutdown()
}
