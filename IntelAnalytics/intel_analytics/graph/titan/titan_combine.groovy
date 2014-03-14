import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Vertex
import org.apache.commons.configuration.BaseConfiguration

combine(args)

def combine(args){
    String[] inputs = args[0].split("\\::")
    conf = new BaseConfiguration()
    conf.setProperty("storage.tablename", inputs[0])
    conf.setProperty("storage.backend", inputs[1])
    conf.setProperty("storage.hostname", inputs[2])
    conf.setProperty("storage.port", inputs[3])
    conf.setProperty("storage.connection_timeout", inputs[4])
    g = TitanFactory.open(conf)
    inputResultPropertyKey = inputs[5].split("\\:")
    type = inputs[6]
    combinedResultPropertyKey = inputs[7].split("\\;")
    biasOn = inputs[8].toBoolean()
    enableStd = inputs[9].toBoolean()
    stdPropertyKey = inputs[10].split("\\;")

    def biasList = []
    def inputResultKeys = []
    def combinedResultKeys = []
    def stdKeys = []
    String avgBiasKeys
    String stdBiasKeys

    numInputResults = inputResultPropertyKey.size()
    (0..<numInputResults).each{
        rawElement = inputResultPropertyKey[it].split(";")
        if(biasOn){
            biasList.add   rawElement[-1]
            if (numInputResults <2)  {
                throw new IllegalArgumentException("The number of input keys should be at least 2 if biasOn!")
            } else {
                inputResultKeys.add rawElement[0..-2]
            }
        }  else {
            inputResultKeys.add rawElement
        }
    }

    if(biasOn){
        if (combinedResultPropertyKey.size() <2)  {
            throw new IllegalArgumentException("The number of combined keys should be at least 2 if biasOn!")
        } else {
            avgBiasKeys = combinedResultPropertyKey[-1]
            combinedResultKeys = combinedResultPropertyKey[0..-2]
        }

        if(enableStd) {
            if (stdPropertyKey.size() <2)  {
                throw new IllegalArgumentException("The number of std keys should be at least 2 if biasOn!")
            } else {
                stdBiasKeys = stdPropertyKey[-1]
                stdKeys = stdPropertyKey[0..-2]
            }
        }
    } else {
        combinedResultKeys = combinedResultPropertyKey
        stdKeys = stdPropertyKey
    }

    for(Vertex v : g.V){
        def avgResults = []
        def results = [][]
        vectorSize = 0
        (0..<numInputResults).each{ i->
            //check if vector_value is used
            elementSize =  inputResultKeys[i].size()
            def result = []
            if (elementSize == 1){
                //vector value was enabled
                //need to get the feature size after query
                oldResults = v.getProperty(inputResultKeys[i][0]).split("[\\s,\\t]+")
                currentSize = oldResults.size()
                if (vectorSize != 0 && vectorSize != currentSize){
                    throw new IllegalArgumentException("The vector size does not match in different results!")
                } else {
                    (0..<currentSize).each{ j->
                        result.add oldResults[j].toDouble()
                    }
                    vectorSize = currentSize
                }
            } else {
                //vector value was not enabled
                if (vectorSize != 0 && vectorSize != elementSize){
                    throw new IllegalArgumentException("The vector size does not match in different results!")
                } else {
                    (0..<elementSize).each{ j->
                        result.add  v.getProperty(inputResultKeys[i][j]).toDouble()
                    }
                    vectorSize = elementSize
                }
            }
            results.add result
        }


        def transResults = results.transpose()

        if (type == 'AVG' || enableStd){
            //get avg on values
            (0..<vectorSize).each{ i->
                avgResults[i] = calAvg(transResults[i])
            }
            combineResultSize =  combinedResultKeys.size()
            //store in vector format
            if (combineResultSize == 1) {
                def resultString =  avgResults.join(",")
                v.setProperty(combinedResultPropertyKey[0], resultString)
            } else {
                //store into different properties
                if(combineResultSize != vectorSize){
                    throw new IllegalArgumentException("The vector size does not match! The vector size of input results is "
                            + vectorSize + ", the vector size of combined results is " + combineResultSize)
                } else {
                    (0..<vectorSize).each{j->
                        v.setProperty(combinedResultKeys[j], avgResults[j])
                    }
                }
            }

            //get avg of bias
            if(biasOn) {
                def biasValues = []
                (0..<biasList.size()).each{
                    biasValues.add v.getProperty(biasList[it]).toDouble()
                }
                avgBias = calAvg(biasValues)
                v.setProperty(avgBiasKeys, avgBias)

                if(enableStd){
                    stdBias = calStd(biasValues, avgBias)
                    v.setProperty(stdBiasKeys, stdBias)
                }
            }
        }



        //calculate standard deviation if configured
        if(enableStd){
            def stdResults = []
            //get avg on values
            (0..<vectorSize).each{
                stdResults[it] = calStd(transResults[it], avgResults[it])
            }

            stdSize =  stdKeys.size()

            //store in vector format
            if (stdSize == 1) {
                def resultString = stdResults.join(",")
                v.setProperty(stdKeys[0], resultString)
            } else {
                //store into different properties
                if(stdSize != vectorSize){
                    throw new IllegalArgumentException("The vector size does not match! The vector size of input results is "
                            + vectorSize + ", the vector size of standard deviation is " + stdSize)
                } else {
                    (0..<vectorSize).each{
                        v.setProperty(stdKeys[it], stdResults[it])
                    }
                }
            }
        }

    }


    g.shutdown()
    println 'complete execution'
}

def calStd(valueList, avgValue){
    stdValue = 0
    (0..<valueList.size()).each{
        stdValue += (valueList[it] - avgValue)**2
    }
    stdValue = Math.sqrt(stdValue)
    return stdValue
}

def calAvg(valueList){
    listSize = valueList.size()
    sumValue = 0
    (0..<listSize).each{
        sumValue += valueList[it]
    }
    avgValue = sumValue/listSize
    return avgValue
}

