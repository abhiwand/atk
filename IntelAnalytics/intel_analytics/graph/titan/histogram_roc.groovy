import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import org.apache.commons.configuration.BaseConfiguration
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.Direction
import groovy.lang.Binding


if(args.length < 1){
    println "================================="
    println "The usage of this program is"
    println "gremlin.sh -e histogram.groovy 'table_name::[property_type]::[enable_roc]::[roc_params]::[prior_property_name]::[posterior_property_name]::[output_path]::[type_key]::[split_types]::[host_name]::[port_num]::[backend]'"
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
String enableRoc = 'false'
String rocParams = "0:0.05:1"
String priorName = '_gb_ID'
String posteriorName = ''
String outputPath = '/tmp/giraph/histogram/'
String key4Type = 'etl-cf:vertex_type'
String[] splitTypes = ['TR','VA','TE']
String hostName = 'localhost'
String port = '2181'
String backend = 'hbase'
float rocMin = 0
float rocStep = 0.05
float rocMax = 1
int rocSize = 21
int splitSize = splitTypes.length
BaseConfiguration conf = new BaseConfiguration()

if(inputs.length > 1){
    propertyType = inputs[1]
}
if(inputs.length > 2){
    enableRoc = inputs[2]
}
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
if(inputs.length > 4){
    priorName = inputs[4]
}
if(inputs.length > 5){
    posteriorName = inputs[5]
}
if(inputs.length > 6){
    outputPath = inputs[6]
}
if(inputs.length > 7){
    key4Type = inputs[7]
}
if(inputs.length > 8){
    splitTypes = inputs[8].toUpperCase().split(',')
    splitSize = splitTypes.length
}
if(inputs.length > 9){
    hostName = inputs[9]
}
if(inputs.length > 10){
    port = inputs[10]
}
if(inputs.length > 11){
    backend = inputs[11]
}
conf.setProperty("storage.tablename", tableName)
conf.setProperty("storage.backend", backend)
conf.setProperty("storage.hostname", hostName)
conf.setProperty("storage.port",port)

//start = System.currentTimeMillis()
Graph g = TitanFactory.open(conf)
if (propertyType == 'VERTEX_PROPERTY'){
    tmp = g.V[0]."$priorName".next()
} else if(propertyType == 'EDGE_PROPERTY') {
    tmp = g.E[0]."$priorName".next()
} else {
    throw new IllegalArgumentException("Input Property Type is not supported!")
}
//println System.currentTimeMillis() - start
int featureSize = tmp.split(' ').length
def rocList = new Object[featureSize][splitSize][rocSize+2]
def sortRocList = new Object[featureSize][splitSize][rocSize+2]
def priorList = []
def posteriorList = []
int[][][] fpCount = new int[featureSize][splitSize][rocSize]
int[][][] tpCount = new int[featureSize][splitSize][rocSize]
int[][][] negCount = new int[featureSize][splitSize][rocSize]
int[][][] posCount = new int[featureSize][splitSize][rocSize]
String priorPath = outputPath + priorName + '.txt'
String posteriorPath = outputPath + posteriorName + '.txt'
String rocPath = outputPath + priorName + "_" + posteriorName + '_roc_'
class roc{
    def threshold
    def fpr
    def tpr
}

class rocCalData{
    def j
    def prior
    def posterior
    def enableRoc
    def featureSize
    def rocSize
    def rocMin
    def rocMax
    def rocStep
    def fpCount
    def tpCount
    def negCount
    def posCount
}

class rocUpdateData{
    def featureSize
    def rocMin
    def rocStep
    def rocSize
    def splitSize
    def rocList
    def sortRocList
    def fpCount
    def tpCount
    def negCount
    def posCount
}
rocCal = new rocCalData(
        j:0,
        prior:0,
        posterior:0,
        enableRoc:enableRoc,
        featureSize:featureSize,
        rocSize:rocSize,
        rocMin:rocMin,
        rocMax:rocMax,
        rocStep:rocStep,
        fpCount:fpCount,
        tpCount:tpCount,
        negCount:negCount,
        posCount:posCount
)

rocUpdates = new rocUpdateData(
        featureSize:featureSize,
        rocMin:rocMin,
        rocStep:rocStep,
        rocSize:rocSize,
        splitSize:splitSize,
        rocList:rocList,
        sortRocList:sortRocList,
        fpCount:fpCount,
        tpCount:tpCount,
        negCount:negCount,
        posCount:posCount
)

if(posteriorName == ''){
    if (propertyType == 'VERTEX_PROPERTY'){
        for(Vertex v : g.V){
           priorList.add(v.getProperty(priorName))
        }
    } else if(propertyType == 'EDGE_PROPERTY') {
        for(Edge e : g.E){
           priorList.add(e.getProperty(priorName))
        }
    } else {
        throw new IllegalArgumentException("Input Property Type is not supported!")
    }
    writeHistogramFile(priorPath, priorList)
} else {
    if (propertyType == 'VERTEX_PROPERTY'){
        //start = System.currentTimeMillis()
        for(Vertex v : g.V){
            for(int j in 0..<splitSize){
                if (v.filter{it.getProperty(key4Type).toUpperCase() == splitTypes[j]}){
                    prior = v.getProperty(priorName)
                    posterior = v.getProperty(posteriorName)
                    priorList.add(prior)
                    posteriorList.add(posterior)
                    rocCal.j = j
                    rocCal.prior = prior
                    rocCal.posterior = posterior
                    calculateRoc(rocCal)
                }
            }
        }
        //println  System.currentTimeMillis() - start
        //start = System.currentTimeMillis()
        updateRoc(rocUpdates)
        //println  System.currentTimeMillis() - start
    } else if(propertyType == 'EDGE_PROPERTY') {
        for(Edge e : g.E){
            for(int j in 0..<splitSize){
                if (e.filter{it.getProperty(key4Type).toUpperCase() == splitTypes[j]}){
                    prior = e.getProperty(priorName)
                    posterior = e.getProperty(posteriorName)
                    priorList.add(prior)
                    posteriorList.add(posterior)
                    rocCal.j = j
                    rocCal.prior = prior
                    rocCal.posterior = posterior
                    calculateRoc(rocCal)
                }
            }
        }
        updateRoc(rocUpdates)
    } else {
        throw new IllegalArgumentException("Input Property Type is not supported!")
    }
    writeHistogramFile(priorPath, priorList)
    writeHistogramFile(posteriorPath, posteriorList)

    if(enableRoc == "true"){
        for(int l in 0..<featureSize){
            for(int m in 0..<splitSize){
                writeRocFile(rocPath + l + '_' + splitTypes[m] + '.txt', sortRocList[l][m])
            }
        }
    }
}
println 'complete execution'


def calculateRoc(rocCal){
    if(rocCal.enableRoc == "true"){
        priorList = rocCal.prior.split()
        posteriorList = rocCal.posterior.split(',')
        for(int i in 0..<rocCal.featureSize){
            for(int k in 0..<rocCal.rocSize){
                tmp = (rocCal.rocMin + rocCal.rocStep*k).round(3)
                rocThreshold = (tmp > rocCal.rocMax? rocCal.rocMax : tmp )
                if(priorList[i].toFloat() >= rocThreshold){
                    rocCal.posCount[i][rocCal.j][k] ++
                    if(posteriorList[i].toFloat() >= rocThreshold) {
                        rocCal.tpCount[i][rocCal.j][k] ++
                    }
                }
                else{
                    rocCal.negCount[i][rocCal.j][k] ++
                    if(posteriorList[i].toFloat() >= rocThreshold) {
                        rocCal.fpCount[i][rocCal.j][k] ++
                    }
                }
            }
        }
    }
}

def updateRoc(rocUpdates){
    for(int i in 0..<rocUpdates.featureSize){
        for(int j in 0..<rocUpdates.splitSize){
            for(int k in 0..<rocUpdates.rocSize) {
                rocUpdates.rocList[i][j][k] = new roc(threshold: (rocUpdates.rocMin + rocUpdates.rocStep*k).round(3),
                        fpr:(rocUpdates.negCount[i][j][k]>0? rocUpdates.fpCount[i][j][k].toFloat()/rocUpdates.negCount[i][j][k].toFloat() : 0),
                        tpr:(rocUpdates.posCount[i][j][k]>0? rocUpdates.tpCount[i][j][k].toFloat()/rocUpdates.posCount[i][j][k].toFloat() : 0))
            }
            rocUpdates.rocList[i][j][rocUpdates.rocSize] = new roc(threshold:0, fpr:0, tpr:0)
            rocUpdates.rocList[i][j][rocUpdates.rocSize+1] = new roc(threshold:1, fpr:1, tpr:1)
            rocUpdates.sortRocList[i][j] = rocUpdates.rocList[i][j].sort{a,b -> a.fpr<=>b.fpr}
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
