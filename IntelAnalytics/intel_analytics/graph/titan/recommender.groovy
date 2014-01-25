import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import org.apache.commons.configuration.BaseConfiguration
import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.Direction
import groovy.lang.Binding

String[] inputs = args[0].split("\\::")
String graphDB = inputs[0]
String vertexID = inputs[1]
String key4Results = inputs[2]
String leftName = inputs[6]
String rightName = inputs[7]
String leftTypeStr = inputs[8]
String rightTypeStr = inputs[9]
String trainStr = inputs[10]
String key4VertexID = inputs[11]
String key4VertexType = inputs[12]
String key4EdgeType = inputs[13]
String[] propertyList = key4Results.tokenize(",")

BaseConfiguration conf = new BaseConfiguration()
conf.setProperty("storage.backend", inputs[3])
conf.setProperty("storage.hostname", inputs[4])
conf.setProperty("storage.port",inputs[5])
conf.setProperty("storage.tablename", graphDB)
Graph g = TitanFactory.open(conf)

Vertex v = g.V(key4VertexID, vertexID).next()
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
        rightTypeStr)

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
              String rightTypeStr) {
    entities = [(leftTypeStr):leftName + '  ', (rightTypeStr):rightName + '  ']
    commonStr = 'Top 10 recommendations to '
    comments = [(leftTypeStr):commonStr + leftName + ': ', (rightTypeStr):commonStr + rightName + ': ']
    vertexType = v.getProperty(key4VertexType)
    recommendType = rightTypeStr
    if (vertexType == rightTypeStr) {
        recommendType = leftTypeStr
    }
    println "================" + comments[vertexType] + vertexID + "================"
    list1 = getResults(v, propertyList)

    def list = []
    count=0
    for(Vertex v2 : g.V.filter{it.getProperty(key4VertexType) == recommendType}) {
        list2 = getResults(v2, propertyList)
        score = calculateScore(list1, list2)
        if (v2.outE.filter{it.getProperty(key4EdgeType) != trainStr}){
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

def getResults(Vertex v, String[] propertyList) {
    def list = []
    length = propertyList.length
    if(length == 0){
      println "ERROR: no property provided for ML result!"
    }

    //firstly add bias
    list.add v.getProperty(propertyList[length-1]).toDouble()
    //then add the results
    for(i=0; i<length-1; i++){
     list.add v.getProperty(propertyList[i]).toDouble()
    }
    return list
}

def calculateScore(list1, list2) {
    sum = list1[0] + list2[0]
    (1..3).each {
        sum += list1[it] * list2[it]
    }
    return sum    
}

