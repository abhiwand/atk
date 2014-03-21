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

    class String:
        """
        String functions
        --------------

**CONCAT(string)** - returns concatenation with given string

**ENDS_WITH(string)** - returns whether the string ends with the given argument

**EQUALS_IGNORE_CASE(string)** - returns whether the string equals the given string, case-insensitive

**INDEX_OF('character', startIndex)** - returns the index of the first occurrence of a character in a string, searching forward from a start index

**LAST_INDEX_OF('character')** - returns the index of the last occurrence of a character in a string, searching backward from the end of the string

**LENGTH()** - returns the length of the string

**LOWER()** - converts all characters in a string to lower case

**LTRIM()** - returns a copy of a string with only leading white space removed

**REGEX_EXTRACT(regex, index)** - performs regular expression matching and extracts the matched group defined by an index parameter

**REGEX_EXTRACT_ALL(regex)** - Performs regular expression matching and extracts all matched groups

**REPLACE(regex, string)** - replaces existing characters with new characters

**RTRIM()** - returns a copy of a string with only trailing white space removed

**STARTSWITH(string)** - determine if the first argument starts with the string in the second

**STRSPLIT(regex, limit)** - splits a string around matches of a given regular expression;
If the limit is positive, the pattern (the compiled representation of the regular expression) is
applied at most limit-1 times, therefore the value of the argument means the maximum length of the
result tuple. The last element of the result tuple will contain all input after the last match.  If the
limit is negative, no limit is applied for the length of the result tuple.  If the limit is zero, no
limit is applied for the length of the result tuple too, and trailing empty strings (if any) will be removed.

**SUBSTRING(startindex, stopIndex)** - returns a substring from a given string;
startIndex is first character of the substring, stopIndex is the index of character *following* the last
character of the substring

**TRIM()** - returns a copy of a string with leading and trailing white space removed

**UPPER()** - returns a string converted to upper case

**TOKENIZE([, 'field_delimiter'])** - splits a string and outputs a bag a words

Examples
--------

>>> # create "result_column" which indicates whether "input_column" ends with "suffix"
>>> frame.transform("input_column", "result_column", EvalFunctions.String.ENDS_WITH, ["suffix"])


>>> # create "result_column" which stores the index of where the character '$' first occurs in the element.
>>> # (stores -1 if '$' does not appear)
>>> frame.transform("input_column", "result_column", EvalFunctions.String.INDEX_OF, ['$', 0])


>>> # Extract the port number from a column of strings like '192.168.0.1:8888'
>>> frame.transform('column_input', 'column_output', EvalFunctions.REGEX_EXTRACT, ['(.*):(.*)', 1])
        """
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

    class Math:
        """
        Math functions
        --------------

**ABS()** - returns the absolute value

**ARITHMETIC()** - performs basic arithmetic operations as specified in source column argument  +, -, *, /, %

**CEIL()** - returns the value rounded up to the nearest integer

**EXP()** - returns e raised to the power of argument

**FLOOR()** - returns the value rounded down to the nearest integer

**LOG()** - returns the natural logarithm (base e)

**LOG10()** - returns the base 10 logarithm

**POW(power)** - returns the  value raised to the power

**RANDOM()** - returns a pseudo random number (input column argument is ignored)

**ROUND()** - returns the value rounded to an integer

**SQRT()** -  returns the positive square root

**STND()** - returns standardized value by subtracting its mean from each element
and dividing this difference by its standard deviation --i.e. ``(element - AVG("input_column"))/STDEV("input_column")``  (see `Standardization <http://en.wikipedia.org/wiki/Feature_scaling#Standardization>`_).

Examples
--------

>>> frame.transform("column_A + column_B", "column_sum", EvalFunctions.Math.ARITHMETIC)
>>> frame.transform("column_A % 2", "column_modulo", EvalFunctions.Math.ARITHMETIC)


>>> # raise the each value in "column_A" to the power of 2, store in "column_power2"
>>> frame.transform("column_A", "column_power2", EvalFunctions.Math.POW, [2])


>>> # generate random number in [1,10] range and save results in "result_column"
>>> frame.transform("input_column", "result_column", EvalFunctions.Math.Random, [1,10])


>>> # calculate standardized value for each element in "input_column" and store in "result_column"
>>> frame.transform("input_column", "result_column", EvalFunctions.Math.STND)
        """
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
        """
        JSON functions
        --------------
        Json.EXTRACT_FIELD

**EXTRACT_FIELD(jsonPath)** -  Extracts a field using JSONPath expression from JSON string which has been imported to a BigDataFrame

Json.EXTRACT_FIELD can be used to extract individual fields from within the JSON strings stored in the BigDataFrame.
The expression syntax follows JSONPath query and needs to return an individual field. Extracing lists are not supported at this time. Extracted fields are stored as strings.

Examples
--------

>>> frame = fb.build_from_json('json_column', 'file.json')
>>> frame.transform('json_column', 'homepage_column', EvalFunctions.Json.EXTRACT_FIELD, ['repository.homepage'])
>>> frame.transform('json_column', 'title_column', EvalFunctions.Json.EXTRACT_FIELD, ['repository.store.book[0].title'])
        """
        EXTRACT_FIELD=2000

    class Xml:
        """
        XML functions
        --------------

**EXTRACT_FIELD(xpath)** -  Extracts a field using XPath expression from XML which has been imported to a BigDataFrame

Xml.EXTRACT_FIELD can be used to extract individual fields from within the XML data stored in the BigDataFrame. Extracted fields are stored as strings.
The expression syntax follows XPath query and needs to return an individual field.

Extracting lists is not supported at this time.

Examples
--------

>>> frame = fb.build_from_xml('xml', 'file.xml', 'repository')
>>> frame.transform('xml_column', 'homepage_column', EvalFunctions.Xml.EXTRACT_FIELD, ['repository/homepage'])
>>> frame.transform('xml_column', 'title_column', EvalFunctions.Xml.EXTRACT_FIELD, ['repository/store/book[1]/title'])
        """
        EXTRACT_FIELD=3000

    class Aggregation:
        """
        Aggregation functions
        ---------------------

**AVG()** - computes the average of the values

**SUM()** - computes the sum of the values

**MAX()** - returns the maximum value

**MIN()** - returns the minimum value

**COUNT()** - returns the number of values present

**DISTINCT()** - returns the distinct values; EvalFunctions.Aggregation.DISTINCT discovers
the values which are distinct and belong to the same group after aggregation.

**DISTINCT_COUNT()** - return the number of unique values

**STDEV()** - computes the standard deviation

**VAR()** - computes the variance

Examples
--------

>>> # group by column_A and compute the average of column_B in each group
>>> # create a new aggregate frame with columns "column_A" and "column_B_avg"
>>> frame.aggregate("column_A", [(EvalFunctions.Aggregation.AVG, "column_B", "column_B_avg")])


>>> # group by column_A and collect all the distinct values in column_B per group
>>> # create a new aggregate frame 'frame_distict' with columns 'column_A' and 'column_B_avg'
>>> aggregated_frame = frame.aggregate('column_A', [(EvalFunctions.Aggregation.DISTINCT, "column_B", "column_B_distinct")], 'frame_distinct')
       """
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
            EvalFunctions.Aggregation.VAR: 'VAR' }

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
