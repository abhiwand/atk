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

from interval import Interval

class ColumnProfile(object):
    """
    Creates a ColumnProfile object to be used for computing statistics on BigColumn object
    Example:
    >>> # Create a ColumnProfile object
    >>> bc = ColumnProfile('column1')
    >>> # Create a ColumnProfile object and specify data intervals for grouping
    >>> # Data Intervals [1..3] (3..6] (6..8) [8..10] (10...) 
    >>> # http://pydoc.net/Python/interval/1.0.0/interval/
    >>> interval_list = [Interval(lower_bound=1, upper_bound=3), 
                         Interval(lower_bound=3, upper_bound=6,  lower_closed=False),
                         Interval(lower_bound=6, upper_bound=8,  closed=False),
                         Interval(lower_bound=8, upper_bound=10),
                         Interval(lower_bound=10,                lower_closed=False)]
    >>> bc = ColumnProfile(data_intervals=interval_list)
    """
    def __init__(self, **kwargs):
        # We intend to add other profile features like pattern, type errors, outlier strategy to this class
        for key in ('data_intervals') : setattr(self, key, kwargs.get(key))
        if self.data_intervals:
            if not all(isinstance(obj, Interval) for obj in self.data_intervals):
                raise Exception('Invalid interval groups specified')

    def get_interval_groups_as_str(self):
        if not self.data_intervals:
            return ""
        return ":".join([repr(x) for x in self.data_intervals])


class BigColumn(object):
    """
    Creates an instance of BigColumn
    Example:
    >>> col1 = BigColumn('col1')
    >>> column_profile = ColumnProfile(data_intervals = [Interval(1,2), Interval(2,3)])
    >>> col2 = BigColumn('col2', profile=column_profile)
    """
    def __init__(self, name, **kwargs):
        """
        Parameters
        ----------
        name: String
            column name
        kwargs: Dictionary
            additional parameters for BigColumn object
        """
        self.name = name
        for key in ('profile') : setattr(self, key, kwargs.get(key))
