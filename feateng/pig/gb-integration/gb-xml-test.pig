REGISTER $PIGGYBANK_LIB;

import '../feature_engineering/pig/gb-integration/common.pig';

-- currently GB processes XML files element by element (instead of line by line)
-- a similar approach can be implemented in Pig with piggybank's XMLLoader
REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar
REGISTER target/Intel-FeatureEngineering-0.0.1-SNAPSHOT.jar;

-- make sure that /tmp/example.xml is in HDFS
characters = LOAD '/tmp/example.xml' USING org.apache.pig.piggybank.storage.XMLLoader('character');
x = FOREACH characters GENERATE xml_element_processor();
DUMP x;