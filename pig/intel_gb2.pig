--MACROS, DEFINES will go here.

DEFINE ExtractJSON com.intel.pig.udf.eval.ExtractJSON();
DEFINE ExtractElement com.intel.pig.udf.eval.ExtractElement();
DEFINE store_graph com.intel.pig.store.RDFStoreFunc('arguments');