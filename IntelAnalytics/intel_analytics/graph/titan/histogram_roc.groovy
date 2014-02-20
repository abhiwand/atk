import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import org.apache.commons.configuration.BaseConfiguration
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.Direction
import groovy.lang.Binding


if(args.length < 1){
    println "================================="
    println "The usage of this program is"
    println "gremlin.sh -e histogram.groovy table_name [property_type] [enable_roc] [roc_params] [prior_property_name] [posterior_property_name] [output_path] [type_key] [split_types] [host_name] [port_num] [backend]"
    println "The arguments in [] are optional."
    println ""
    println "The default value for property_type is VERTEX_PROPERTY"
    println "The default value for enable_roc is false"
    println "The default value for roc_params is 0:0.05:1"
    println "The default value for prior_property_name is _gb_ID"
    println "The default value for posterior_property_name is ''"
    println "The default value for output_path is /tmp/giraph/histogram"
    println "The default value for type_key is etl-cf:vertex_type"
    println "The default value for split_types is TR,VA,TE"
    println "The default value for host_name is localhost"
    println "The default value for port_num is 2181"
    println "The default value for backend is hbase"
    println ""
    throw new IllegalArgumentException("Please at least inputs table name to run this program!")
}
String[] inputs = args[0].split("\\::")
String tableName = inputs[0]
String propertyType = 'VERTEX_PROPERTY'
if(inputs.length > 1){
    propertyType = inputs[1]
}
String enableRoc = 'false'
if(inputs.length > 2){
    enableRoc = inputs[2]
}
String rocParams = "0:0.05:1"
float rocMin = 0
float rocStep = 0.05
float rocMax = 1
int rocSize = 21
if(inputs.length > 3){
    rocParams = inputs[3]
    String[] rocList =  rocParams.split('\\:')
    if(rocList.length != 3){
        throw new IllegalArgumentException("Please input roc threshold in 'min:step:max' format")
    } else {
        rocMin = Float.parseFloat(rocList[0])
        rocStep = Float.parseFloat(rocList[1])
        rocMax = Float.parseFloat(rocList[2])
        rocSize = (rocMax - rocMin)/rocStep + 2
    }
}

String priorName = '_gb_ID'
if(inputs.length > 4){
    priorName = inputs[4]
}
String posteriorName = ''
if(inputs.length > 5){
    posteriorName = inputs[5]
}
String outputPath = '/tmp/giraph/histogram/'
if(inputs.length > 6){
    outputPath = inputs[6]
}
String key4Type = 'etl-cf:vertex_type'
if(inputs.length > 7){
    key4Type = inputs[7]
}
String[] splitTypes = ['TR','VA','TE']
if(inputs.length > 8){
    splitTypes = inputs[8].toUpperCase().split(',')
}
int splitSize = splitTypes.length
String hostName = 'localhost'
if(inputs.length > 9){
    hostName = inputs[9]
}
String port = '2181'
if(inputs.length > 10){
    port = inputs[10]
}
String backend = 'hbase'
if(inputs.length > 11){
    backend = inputs[11]
}

BaseConfiguration conf = new BaseConfiguration()
conf.setProperty("storage.tablename", tableName)
conf.setProperty("storage.backend", backend)
conf.setProperty("storage.hostname", hostName)
conf.setProperty("storage.port",port)

Graph g = TitanFactory.open(conf)
if (propertyType == 'VERTEX_PROPERTY'){
    tmp = g.V[0]."$priorName".next()
} else if(propertyType == 'EDGE_PROPERTY') {
    tmp = g.E[0]."$priorName".next()
} else {
    throw new IllegalArgumentException("Input Property Type is not supported!")
}
int featureSize = tmp.split(' ').length
def list1 = []
def list2 = []
def list3 = new Object[featureSize][splitSize][rocSize+2]
def sortList3 = new Object[featureSize][splitSize][rocSize+2]
def priorList = []
def posteriorList = []
int[][][] fpCount = new int[featureSize][splitSize][rocSize]
int[][][] tpCount = new int[featureSize][splitSize][rocSize]
int[][][] negCount = new int[featureSize][splitSize][rocSize]
int[][][] posCount = new int[featureSize][splitSize][rocSize]
String outputPath1 = outputPath + priorName + '.txt'
String outputPath2 = outputPath + posteriorName + '.txt'
String outputPath3 = outputPath + priorName + "_" + posteriorName + '_roc_'
class roc{
    def threshold
    def fpr
    def tpr
}

if(posteriorName == ''){
    if (propertyType == 'VERTEX_PROPERTY'){
        for(Vertex v : g.V){
           list1.add(v.getProperty(priorName))
        }
    } else if(propertyType == 'EDGE_PROPERTY') {
        for(Edge e : g.E){
           list1.add(e.getProperty(priorName))
        }
    } else {
        throw new IllegalArgumentException("Input Property Type is not supported!")
    }
    writeHistogramFile(outputPath1, list1)
} else {
    if (propertyType == 'VERTEX_PROPERTY'){
        for(Vertex v : g.V){
            for(int j in 0..<splitSize){
                if (v.filter{it.getProperty(key4Type).toUpperCase() == splitTypes[j]}){
                    prior = v.getProperty(priorName)
                    posterior = v.getProperty(posteriorName)
                    list1.add(prior)
                    list2.add(posterior)
                    calculateRoc(j,
                            prior,
                            posterior,
                            enableRoc,
                            featureSize,
                            rocSize,
                            rocMin,
                            rocMax,
                            rocStep,
                            fpCount,
                            tpCount,
                            negCount,
                            posCount)
                }
            }
        }
        updateRoc(featureSize,
                rocMin,
                rocStep,
                rocSize,
                splitSize,
                list3,
                sortList3,
                fpCount,
                tpCount,
                negCount,
                posCount)
    } else if(propertyType == 'EDGE_PROPERTY') {
        for(Edge e : g.E){
            for(int j in 0..<splitSize){
                if (e.filter{it.getProperty(key4Type).toUpperCase() == splitTypes[j]}){
                    prior = e.getProperty(priorName)
                    posterior = e.getProperty(posteriorName)
                    list1.add(prior)
                    list2.add(posterior)
                    calculateRoc(j,
                            prior,
                            posterior,
                            enableRoc,
                            featureSize,
                            rocSize,
                            rocMin,
                            rocMax,
                            rocStep,
                            fpCount,
                            tpCount,
                            negCount,
                            posCount)
                }
            }
        }
        updateRoc(featureSize,
                rocMin,
                rocStep,
                rocSize,
                splitSize,
                list3,
                sortList3,
                fpCount,
                tpCount,
                negCount,
                posCount)
    } else {
        throw new IllegalArgumentException("Input Property Type is not supported!")
    }
    writeHistogramFile(outputPath1, list1)
    writeHistogramFile(outputPath2, list2)

    if(enableRoc == "true"){
        for(int l in 0..<featureSize){
            for(int m in 0..<splitSize){
                writeRocFile(outputPath3 + l + '_' + splitTypes[m] + '.txt', sortList3[l][m])
            }
        }
    }

}


def calculateRoc(j,
                 priorInfo,
                 posteriorInfo,
                 enableRoc,
                 featureSize,
                 rocSize,
                 rocMin,
                 rocMax,
                 rocStep,
                 fpCount,
                 tpCount,
                 negCount,
                 posCount){
    if(enableRoc == "true"){
        priorList = priorInfo.split()
        posteriorList = posteriorInfo.split(',')
        for(int i in 0..<featureSize){
            for(int k in 0..<rocSize){
                tmp = (rocMin + rocStep*k).round(3)
                rocThreshold = (tmp > rocMax? rocMax : tmp )
                if(priorList[i].toFloat() >= rocThreshold){
                    posCount[i][j][k] ++
                    if(posteriorList[i].toFloat() >= rocThreshold) {
                        tpCount[i][j][k] ++
                    }
                }
                else{
                    negCount[i][j][k] ++
                    if(posteriorList[i].toFloat() >= rocThreshold) {
                        fpCount[i][j][k] ++
                    }
                }
            }
        }
    }
}

def updateRoc(featureSize,
              rocMin,
              rocStep,
              rocSize,
              splitSize,
              list3,
              sortList3,
              fpCount,
              tpCount,
              negCount,
              posCount){
    for(int i in 0..<featureSize){
        for(int j in 0..<splitSize){
            for(int k in 0..<rocSize) {
                list3[i][j][k] = new roc(threshold: (rocMin + rocStep*k).round(3),
                        fpr:(negCount[i][j][k]>0? fpCount[i][j][k].toFloat()/negCount[i][j][k].toFloat() : 0),
                        tpr:(posCount[i][j][k]>0? tpCount[i][j][k].toFloat()/posCount[i][j][k].toFloat() : 0))
            }
            list3[i][j][rocSize] = new roc(threshold:0, fpr:0, tpr:0)
            list3[i][j][rocSize+1] = new roc(threshold:1, fpr:1, tpr:1)
            sortList3[i][j] = list3[i][j].sort{a,b -> a.fpr<=>b.fpr}
        }
    }
}

void writeHistogramFile(def path, def infoList) {
    new File("$path").withWriter { out ->
        infoList.each {
            out.println it
        }
    }
}

void writeRocFile(def path, def infoList) {
    new File("$path").withWriter { out ->
        infoList.each {
            out.println it.fpr + '\t' + it.tpr + '\t' + it.threshold
        }
    }
}
