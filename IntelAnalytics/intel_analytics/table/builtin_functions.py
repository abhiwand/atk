##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
The Builtin functions that can be applied with the transform method on BigDataFrames.
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
        CONCAT=18   #CONCAT is part of Pig Eval functions
        
    """Math functions
    """        
    class Math:
        ABS=1000
        LOG=1001
        LOG10=1002
        POW=1003
        EXP=1004
        STND=1005 #STND: Standardization (see http://en.wikipedia.org/wiki/Feature_scaling#Standardization).

        # Arithmetic operations, e.g., +-*/%?, syntax checking is left to pig script engine:
        ARITHMETIC=1100

        FLOOR=1006
        CEIL=1007
        ROUND=1008
        SQRT=1009
        DIV=1010
        MOD=1011
        RANDOM=1012
        
    class Json:
        EXTRACT_FIELD=2000


    class DateTime:
        ADDDURATION = 4000
        CURRENTTIME = 4001

        GETDAY = 4003
        GETHOUR = 4004
        GETMILLISECOND = 4005
        GETMINUTE = 4006
        GETMONTH = 4007
        GETSECOND = 4008
        GETWEEK = 4009
        GETWEEKYEAR = 4010
        GETYEAR = 4011


        SUBTRACTDURATION = 4017
        TODATE = 4018
        TOMILLISECONDS = 4019
        TOSTRING = 4020
        TOUNIXTIME = 4021
        BEFOREDATE = 4022
        AFTERDATE = 4023
        DAYOFTHEYEAR = 4024
        ORDINALDATE = 4025

    class Xml:
	    EXTRACT_FIELD=3000

    class Aggregation:
        AVG=5000
        SUM=5001
        MAX=5002
        MIN=5003
        COUNT=5004
        DISTINCT=5005
        COUNT_DISTINCT=5006
        STDEV=5007 #Population standard deviation
        VAR=5008
	

    @staticmethod
    def to_string(x):
        #these strings will be passed to the pig jython scripts
        mapping = {
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
            EvalFunctions.String.CONCAT: 'CONCAT',

            EvalFunctions.Math.ABS: 'ABS',
            EvalFunctions.Math.LOG: 'LOG',
            EvalFunctions.Math.LOG10: 'LOG10',
            EvalFunctions.Math.POW: 'org.apache.pig.piggybank.evaluation.math.POW',
            EvalFunctions.Math.EXP: 'EXP',
            EvalFunctions.Math.STND: 'STND',
            EvalFunctions.Math.FLOOR: 'FLOOR',
            EvalFunctions.Math.CEIL: 'CEIL',
            EvalFunctions.Math.ROUND: 'ROUND',
            EvalFunctions.Math.SQRT: 'SQRT',
            EvalFunctions.Math.DIV: 'DIV',
            EvalFunctions.Math.MOD: 'MOD',
            EvalFunctions.Math.RANDOM: 'RANDOM',
            EvalFunctions.Math.ARITHMETIC: 'ARITHMETIC',

            EvalFunctions.Json.EXTRACT_FIELD: 'com.intel.pig.udf.ExtractJSON',
            EvalFunctions.Xml.EXTRACT_FIELD: 'org.apache.pig.piggybank.evaluation.xml.XPath',

            EvalFunctions.Aggregation.AVG: 'AVG',
            EvalFunctions.Aggregation.SUM: 'SUM',
            EvalFunctions.Aggregation.MAX: 'MAX',
            EvalFunctions.Aggregation.MIN: 'MIN',
            EvalFunctions.Aggregation.COUNT: 'COUNT',
            EvalFunctions.Aggregation.DISTINCT: 'DISTINCT',
            EvalFunctions.Aggregation.COUNT_DISTINCT: 'COUNT_DISTINCT',
            EvalFunctions.Aggregation.STDEV: 'STDEV',
            EvalFunctions.Aggregation.VAR: 'VAR',

            EvalFunctions.DateTime.ADDDURATION: 'AddDuration',
            EvalFunctions.DateTime.CURRENTTIME: 'CurrentTime',
            EvalFunctions.DateTime.GETDAY: 'GetDay',
            EvalFunctions.DateTime.GETHOUR: 'GetHour',
            EvalFunctions.DateTime.GETMILLISECOND: 'GetMilliSecond',
            EvalFunctions.DateTime.GETMINUTE: 'GetMinute',
            EvalFunctions.DateTime.GETMONTH: 'GetMonth',
            EvalFunctions.DateTime.GETSECOND: 'GetSecond',
            EvalFunctions.DateTime.GETWEEK: 'GetWeek',
            EvalFunctions.DateTime.GETWEEKYEAR: 'GetWeekYear',
            EvalFunctions.DateTime.GETYEAR: 'GetYear',
            EvalFunctions.DateTime.SUBTRACTDURATION: 'SubtractDuration',
            EvalFunctions.DateTime.TODATE: 'ToDate',
            EvalFunctions.DateTime.TOMILLISECONDS: 'ToMilliSeconds',
            EvalFunctions.DateTime.TOSTRING: 'ToString',
            EvalFunctions.DateTime.TOUNIXTIME: 'ToUnixTime',
            EvalFunctions.DateTime.BEFOREDATE: 'com.intel.pig.udf.BeforeDate',
            EvalFunctions.DateTime.AFTERDATE: 'com.intel.pig.udf.AfterDate',
            EvalFunctions.DateTime.DAYOFTHEYEAR: 'com.intel.pig.udf.DayOfTheYear',
            EvalFunctions.DateTime.ORDINALDATE: 'com.intel.pig.udf.OrdinalDate'
        }

        if x in mapping:
            return mapping[x]
        else:
            raise Exception("The function specified is not valid")

string_functions = []
math_functions = []  
json_functions = []
xml_functions = []
aggregation_functions= []
available_builtin_functions = []#used for validation, does the user try to call a valid function?
datetime_functions = []

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

for key,val in EvalFunctions.DateTime.__dict__.items():
    if key == '__module__' or key == '__doc__':
        continue
    datetime_functions.append(EvalFunctions.to_string(val))

for key,val in EvalFunctions.Xml.__dict__.items():
    if key == '__module__' or key == '__doc__':
        continue
    xml_functions.append(EvalFunctions.to_string(val))

for key,val in EvalFunctions.Aggregation.__dict__.items():
    if key == '__module__' or key == '__doc__':
        continue
    aggregation_functions.append(EvalFunctions.to_string(val))

available_builtin_functions.extend(string_functions)
available_builtin_functions.extend(math_functions)
available_builtin_functions.extend(json_functions)
available_builtin_functions.extend(xml_functions)
available_builtin_functions.extend(aggregation_functions)
available_builtin_functions.extend(datetime_functions)
