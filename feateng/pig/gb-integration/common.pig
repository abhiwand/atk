-- REGISTER statements are not supported yet in macros
DEFINE generate_graph com.intel.pig.gb.GraphEvalFunc(); --parses the input data to produce graph elements
DEFINE xml_element_processor com.intel.pig.gb.XMLParserEvalFunc();--sample XML parser that parses XML elements extracted from the input file