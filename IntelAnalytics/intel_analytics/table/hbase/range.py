##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
import re

def _strictly_increasing(L):
    return all(a<b for a, b in zip(L, L[1:]))

def _strictly_decreasing(L):
    return all(a>b for a, b in zip(L, L[1:]))

def _is_valid_range(min, max, stepsize):
    result = True
    if ((min > max and stepsize > 0) or 
        (min < max and stepsize < 0) or
        (min == max) or
        (stepsize == 0)):
        result = False
    return result

class ETLRange:

    __range_timeseries = 1	# For inputs of type day, month etc.
    __range_stepint = 2		# For inputs of type min, max, stepsize
    __range_stepfloat = 3	# For inputs of type min, max, stepsize
    __range_csv = 4		# For input values separated by comma
    __range_mixed = 5		# For mixed input e.g. 1,4:10:2,12
    __range_depth = 6		# For ranges as depths

    __float_format = "[-+]?[0-9]+(\.[0-9]+)?"                              		# decimal pattern
    __int_format = "-?[0-9]+"                                              		# number pattern
    __step_float_format = "%s:%s:%s" % (__float_format,__float_format,__float_format)	# step pattern with floats
    __step_int_format   = "%s:%s:%s" % (__int_format,__int_format,__int_format)	        # step pattern with ints


    def __init__(self, range):
        self.range_expression = "".join(range.split())		  # Remove all white space characters
	
	d,n,sd,sn = ETLRange.__float_format, ETLRange.__int_format, ETLRange.__step_float_format, ETLRange.__step_int_format
 
        if (self.range_expression in ["date","week","month","year","dayofweek","monthofyear"]):
            self.range_type = ETLRange.__range_timeseries
        elif (re.match("^%s$" % (sn), self.range_expression)):
            self.range_type = ETLRange.__range_stepint
        elif (re.match("^%s$" % (sd), self.range_expression)):
            self.range_type = ETLRange.__range_stepfloat
        elif (re.match("^%s(,%s)+$" % (d,d),  self.range_expression)):
            self.range_type = ETLRange.__range_csv
        elif (re.match("^(%s|%s)(,(%s|%s))+$" % (d,sd,d,sd),  self.range_expression)):
            self.range_type = ETLRange.__range_mixed
        elif (re.match("^%s$" % (d),  self.range_expression)):  
            self.range_type = ETLRange.__range_depth
            raise Exception('Unsupported feature: depth as a range')
        else:
            raise Exception('Invalid range %s' % (range))

      
    def toString(self):
        """
        returns the range_expression as a string based on its type
        """
        range_str = ''
        if (self.range_type == ETLRange.__range_timeseries):
            range_str = self.range_expression

        elif (self.range_type == ETLRange.__range_csv):
            L = [float(x) for x in self.range_expression.split(',')]
            if (_strictly_increasing(L) or _strictly_decreasing(L)):
                range_str = self.range_expression
            else:
                raise Exception('Invalid range: Range should be strictly increasing or decreasing')		

        elif (self.range_type == ETLRange.__range_stepint):
            limits = [(f.strip()) for f in self.range_expression.split(':')]
            min, max, stepsize = int(limits[0]), int(limits[1]), int(limits[2])
            if (not _is_valid_range(min, max, stepsize)):
                raise Exception('Illegal range %s' % (self.range_expression))
            range_str = ','.join(str(i) for i in xrange(min, max, stepsize)) 
            range_str += ",%d" % (max)

        elif (self.range_type == ETLRange.__range_stepfloat):
            limits = [(f.strip()) for f in self.range_expression.split(':')]
            min, max, stepsize = float(limits[0]), float(limits[1]), float(limits[2])
            if (not _is_valid_range(min, max, stepsize)):
                raise Exception('Illegal range %s' % (self.range_expression))
            r = min
            while r < max:
                range_str += "%f," %(r)
                r += stepsize
            range_str += "%f" %(max)

        elif (self.range_type == ETLRange.__range_mixed):
	    d,sd = ETLRange.__float_format, ETLRange.__step_float_format
	    range_list = []
            for i in self.range_expression.split(','):
                if (re.match("^%s$" % (d),i)):
                    range_list.append(i)
                elif (re.match("^%s$" % (sd),i)):
                    range_list.append(ETLRange(i).toString())
            range_str = ",".join(range_list)

        return range_str
