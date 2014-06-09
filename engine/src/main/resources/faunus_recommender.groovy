import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import org.apache.commons.configuration.BaseConfiguration



class recommendation {
    def id
    def rec
}

def g

list1 = []
list3 = []


def setup(args){
    String[] inputs = args[0].split("\\::")
    conf = new BaseConfiguration()
    conf.setProperty("storage.tablename", inputs[0])
    conf.setProperty("storage.backend", inputs[3])
    conf.setProperty("storage.hostname", inputs[4].replaceAll('\\;',','))
    conf.setProperty("storage.port",inputs[5])
    conf.setProperty("storage.connection_timeout", inputs[6])
    g = TitanFactory.open(conf)

    String vertexID = inputs[1]
    String key4Results = inputs[2]
    String leftName = inputs[7]
    String rightName = inputs[8]
    String leftTypeStr = inputs[9].toUpperCase()
    String rightTypeStr = inputs[10].toUpperCase()
    String key4VertexID = inputs[12]
    String key4VertexType = inputs[13]
    String vertexType = inputs[15].toUpperCase()
    String vectorValue = inputs[16]
    String biasOn = inputs[17]
    String outputPath = inputs[19]
    String[] propertyList = key4Results.tokenize(";")

    output = new File("$outputPath")

    //Get vertex based on its original input vertex Id
    //and the vertex type
    Vertex v = g.V(key4VertexID, vertexID).
            filter{it.getProperty(key4VertexType).toUpperCase() == vertexType}.next()

    entities = [(leftTypeStr):leftName + '  ', (rightTypeStr):rightName + '  ']
    commonStr = 'Top 10 recommendations to '
    comments = [(leftTypeStr):commonStr + leftName + ': ', (rightTypeStr):commonStr + rightName + ': ']

    output.append("================" + comments[vertexType] + vertexID + "================\n")
    list1 = getResults(v, propertyList, vectorValue, biasOn)
}

def map(v,args){
    String[] inputs = args[0].split("\\::")
    String key4Results = inputs[2]
    String leftTypeStr = inputs[9].toUpperCase()
    String rightTypeStr = inputs[10].toUpperCase()
    String trainStr = inputs[11].toUpperCase()
    String key4VertexID = inputs[12]
    String key4VertexType = inputs[13]
    String key4EdgeType = inputs[14]
    String vertexType = inputs[15].toUpperCase()
    String vectorValue = inputs[16]
    String biasOn = inputs[17]
    Integer featureDimension = inputs[18].toInteger()
    String[] propertyList = key4Results.tokenize(";")

    recommendType = rightTypeStr
    if (vertexType == rightTypeStr) {
        recommendType = leftTypeStr
    }

    u = g.v(v.id)

    if(//u.hasProperty(key4VertexType) &&
       u.getProperty(key4VertexType).toUpperCase() == recommendType &&
       u.outE.filter{it.getProperty(key4EdgeType).toUpperCase() != trainStr}){
        list2 = getResults(u, propertyList, vectorValue, biasOn)
        score = calculateScore(list1, list2, biasOn, featureDimension)
        list3.add new recommendation(id:u.getProperty(key4VertexID), rec:score)
    }
}

def getResults(u, propertyList, vectorValue, biasOn) {
    def list = []
    length = propertyList.length
    valueLength = length
    if(length == 0){
        println "ERROR: no property provided for ML result!"
    } else if (vectorValue == "true"  &&
            biasOn == "true"  &&
            length != 2){
        println "ERROR: wrong property length provided for ML result!"
    }

    //firstly add bias
    if(biasOn == "true"){
        list.add u.getProperty(propertyList[length-1]).toDouble()
        valueLength = length - 1
    }

    //then add the results
    if(vectorValue == "true"){
         values = u.getProperty(propertyList[0]).split("[\\s,\\t]+")
         for(i in 0..<values.size()){
            list.add values[i].toDouble()
        }
    } else {
        for(i in 0..<valueLength){
            list.add u.getProperty(propertyList[i]).toDouble()
        }
    }

    return list
}

def calculateScore(list1, list2, biasOn, featureDimension) {
    sum = 0
    if (list1.size() > 0 && list2.size()>0 ){
        if(biasOn == "true"){
            sum = list1[0] + list2[0]
            (1..featureDimension).each {
                sum += list1[it] * list2[it]
            }
        } else {
            (0..<featureDimension).each {
                sum = list1[it] * list2[it]
            }
        }
    }
    return sum
}

// close the Titan database connection
def cleanup(args) {
    listSize = list3.size()
    if (listSize > 0){
        size = listSize >= 10? 10 : listSize
        sortedList = list3.sort{a,b -> b.rec<=>a.rec}[0..<size]
        writeRecommenderFile(sortedList, entities[recommendType])
    }
    g.shutdown()
}


void writeRecommenderFile(def infoList, def entity) {
    String[] inputs = args[0].split("\\::")
    String outputPath = inputs[19]
    /*
    output.withWriter { out ->
        infoList.each {
            out.println entity + it.id + "  score " + it.rec
        }
    }   */

    output = new File("$outputPath")
    String content =''
    infoList.each {
        content += entity + it.id + "  score " + it.rec + "\n"
    }
    output.append(content)
}

