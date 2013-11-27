"""
Builtin functions that can be applied with the transform method on BigDataFrames
"""
class EvalFunctions:
    """String functions
    """
    class String:
        ENDS_WITH=1
        EQUALS_IGNORE_CASE=2
        INDEX_OF=3
        LAST_INDEX_OF=4
        LOWER=5
        LTRIM=6  
        REGEX_EXTRACT=7  
        REGEX_EXTRACT_ALL=8 
        REPLACE=9
        RTRIM=10
        STARTS_WITH=11 
        STRSPLIT=12
        SUBSTRING=13
        TRIM=14
        UPPER=15
        TOKENIZE=16
        LENGTH=17
        
    """Math functions
    """        
    class Math:
        ABS=1000
        LOG=1001
        LOG10=1002
        POW=1003
        EXP=1004
        STND=1005 #STND: standardization (see http://en.wikipedia.org/wiki/Feature_scaling#Standardization)
        
    class Json:
        EXTRACT_FIELD=2000

    @staticmethod
    def to_string(x):
        #these strings will be passed to the pig jython scripts
        return {
            EvalFunctions.String.ENDS_WITH: 'ENDSWITH',
            EvalFunctions.String.EQUALS_IGNORE_CASE: 'EqualsIgnoreCase',
            EvalFunctions.String.INDEX_OF: 'INDEXOF',
            EvalFunctions.String.LAST_INDEX_OF: 'LAST_INDEX_OF',
            EvalFunctions.String.LOWER: 'LOWER',
            EvalFunctions.String.LTRIM: 'LTRIM',      
            EvalFunctions.String.REGEX_EXTRACT: 'REGEX_EXTRACT',      
            EvalFunctions.String.REGEX_EXTRACT_ALL: 'REGEX_EXTRACT_ALL',
            EvalFunctions.String.REPLACE: 'REPLACE',
            EvalFunctions.String.RTRIM: 'RTRIM',
            EvalFunctions.String.STARTS_WITH: 'STARTSWITH',
            EvalFunctions.String.STRSPLIT: 'STRSPLIT',
            EvalFunctions.String.SUBSTRING: 'SUBSTRING',
            EvalFunctions.String.TRIM: 'TRIM',
            EvalFunctions.String.UPPER: 'UPPER',
            EvalFunctions.String.TOKENIZE: 'TOKENIZE',
            EvalFunctions.String.LENGTH: 'org.apache.pig.piggybank.evaluation.string.LENGTH',

            EvalFunctions.Math.ABS: 'ABS',
            EvalFunctions.Math.LOG: 'LOG',
            EvalFunctions.Math.LOG10: 'LOG10',
            EvalFunctions.Math.POW: 'org.apache.pig.piggybank.evaluation.math.POW',
            EvalFunctions.Math.EXP: 'EXP',    
            EvalFunctions.Math.STND: 'STND',
            
            EvalFunctions.Json.EXTRACT_FIELD: 'com.intel.pig.udf.ExtractJSON'
        }[x]

string_functions = []
math_functions = []  
json_functions = []
available_builtin_functions = []#used for validation, does the user try to call a valid function? 
for key,val in EvalFunctions.String.__dict__.items():
    if key == '__module__' or key == '__doc__':
        continue
    string_functions.append(EvalFunctions.to_string(val))
    
for key,val in EvalFunctions.Math.__dict__.items():
    if key == '__module__' or key == '__doc__':
        continue
    math_functions.append(EvalFunctions.to_string(val))  
    
for key,val in EvalFunctions.Json.__dict__.items():
    if key == '__module__' or key == '__doc__':
        continue
    json_functions.append(EvalFunctions.to_string(val)) 

available_builtin_functions.extend(string_functions)
available_builtin_functions.extend(math_functions)
available_builtin_functions.extend(json_functions)