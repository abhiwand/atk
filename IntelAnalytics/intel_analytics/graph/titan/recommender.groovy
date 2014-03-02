import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import org.apache.commons.configuration.BaseConfiguration

String[] inputs = args[0].split("\\::")
String graphDB = inputs[0]
String vertexID = inputs[1]
String key4Results = inputs[2]
String leftName = inputs[6]
String rightName = inputs[7]
String leftTypeStr = inputs[8].toUpperCase()
String rightTypeStr = inputs[9].toUpperCase()
String trainStr = inputs[10].toUpperCase()
String key4VertexID = inputs[11]
String key4VertexType = inputs[12]
String key4EdgeType = inputs[13]
String vertexType = inputs[14].toUpperCase()
String vectorValue = inputs[15]
String biasOn = inputs[16]
Integer featureDimension = inputs[17].toInteger()
String[] propertyList = key4Results.tokenize(",")

BaseConfiguration conf = new BaseConfiguration()
conf.setProperty("storage.backend", inputs[3])
conf.setProperty("storage.hostname", inputs[4])
conf.setProperty("storage.port",inputs[5])
conf.setProperty("storage.tablename", graphDB)
Graph g = TitanFactory.open(conf)

//Get vertex based on its original input vertex Id
//and the vertex type
Vertex v = g. V(key4VertexID, vertexID).
        filter{it.getProperty(key4VertexType).toUpperCase() == vertexType}.next()

recommend(v,
        g,
        propertyList,
        leftName,
        rightName,
        key4VertexType,
        key4VertexID,
        vertexID,
        key4EdgeType,
        trainStr,
        leftTypeStr,
        rightTypeStr,
        vertexType,
        vectorValue,
        biasOn,
        featureDimension)

def recommend(Vertex v,
              Graph g,
              String[] propertyList,
              String leftName,
              String rightName,
              String key4VertexType,
              String key4VertexID,
              String vertexID,
              String key4EdgeType,
              String trainStr,
              String leftTypeStr,
              String rightTypeStr,
              String vertexType,
              String vectorValue,
              String biasOn,
              Integer featureDimension) {
    entities = [(leftTypeStr):leftName + '  ', (rightTypeStr):rightName + '  ']
    commonStr = 'Top 10 recommendations to '
    comments = [(leftTypeStr):commonStr + leftName + ': ', (rightTypeStr):commonStr + rightName + ': ']
    //vertexType = v.getProperty(key4VertexType)
    recommendType = rightTypeStr
    if (vertexType == rightTypeStr) {
        recommendType = leftTypeStr
    }

    println "================" + comments[vertexType] + vertexID + "================"
    list1 = getResults(v, propertyList, vectorValue, biasOn)

    def list = []
    for(Vertex v2 : g.V.filter{(it.getProperty(key4VertexType)).toUpperCase() == recommendType}) {
        list2 = getResults(v2, propertyList, vectorValue, biasOn)
        score = calculateScore(list1, list2, biasOn, featureDimension)
        if (v2.outE.filter{it.getProperty(key4EdgeType).toUpperCase() != trainStr}){
            list.add new recommendation(id:v2.getProperty(key4VertexID), rec:score)
        }
    }
    sortedlist = list.sort{a,b -> b.rec<=>a.rec}[0..10]
    (0..10).each{ println entities[recommendType] + sortedlist[it].id + "  score " + sortedlist[it].rec }
    println 'complete recommend'
}

class recommendation {
   def id
   def rec
}

def getResults(Vertex v, String[] propertyList, String vectorValue, String biasOn) {
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
      list.add v.getProperty(propertyList[length-1]).toDouble()
      valueLength = length - 1
    }

    //then add the results
    if(vectorValue == "true"){
      values = v.getProperty(propertyList[0]).split("[\\s,\\t]+")
        for(i in 0..<values.size()){
            list.add values[i].toDouble()
        }
    } else {
        for(i in 0..<valueLength){
            list.add v.getProperty(propertyList[i]).toDouble()
        }
    }

    return list
}

def calculateScore(list1, list2, biasOn, featureDimension) {
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

    return sum    
}

