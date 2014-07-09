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
import warnings
from ordereddict import OrderedDict
from collections import defaultdict
from iatypes import supported_types


class DataFile(object):
    annotation = "data_file"
    pass


class CsvFile(DataFile):
    """
    Creates object which defines a CSV file.

    Parameters
    ----------
    file_name : string
        name of data input file
    schema : list of tuples of the form (string, type)
        schema description of the fields for a given line.  It is a list of
        tuples which describe each field, (field name, field type), where
        the field name is a string, and file is a supported type
        (See supported_types from the types module)
        The type ``ignore`` may also be used if the field should be ignored
        on loads
    delimiter : string
        string indicator of the delimiter for the fields, the comma character is the default
    skip_header_lines : int32
        indicates numbers of lines to skip before parsing records

    Raises
    ------
    ValueError
        * "file_name must be a non-empty string": check for spurious leading comma in the parameters
        * "schema must be non-empty list of tuples": check for spelling errors in the creation, building and application of the schema variable
        * "delimiter must be a non-empty string": "" is not a valid delimiter between columns
        * "First item in CSV schema tuple must be a string": check schema 
        * "Second item in CSV schema tuple must be a supported type: ...": check schema

    Examples
    --------
    >>>
    For this example, we are going to use a raw data file named "raw_data.csv". The data is described in the first tow rows. The first row is the column names: 'a', 'b', 'c', and the second row has the column types: <type 'int32'>, <type 'int32'>, <type 'str'>.
    <BLANKLINE>
    First bring in the stuff.
    >>> from intelanalytics import *
    <BLANKLINE>
    At this point create a schema that defines the data
    >>> csv_schema = [("a", int32),
    ...               ("b", int32),
    ...               ("c", string)]
    <BLANKLINE>
    Now build a CsvFile object with this schema
    >>> csv_define = CsvFile("raw_data.csv", csv_schema)
    <BLANKLINE>
    The standard delimiter in a csv file is the comma. If the columns of data were separated by a character other than comma, we need to add the appropriate delimiter. For example if the data columns were separated by the colon character, the instruction would be:
    >>> csv_data = CsvFile("raw_data.csv", csv_schema, ':') # or CsvFile("raw_data.csv", csv_schema, delimiter = ':')
    <BLANKLINE>
    Our example data file had two lines at the top which were not data.
    Therefore, we should have skipped these lines.
    >>> csv_data = CsvFile("<path to>raw_data.csv", csv_schema, skip_header_lines=2)

    """

    # TODO - Review docstring
    annotation = "csv_file"

    def __init__(self, file_name, schema, delimiter=',', skip_header_lines=0):
        if not file_name  or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        if not schema:
            raise ValueError("schema must be non-empty list of tuples")
        if not delimiter or not isinstance(delimiter, basestring):
            raise ValueError("delimiter must be a non-empty string")
        self.file_name = file_name
        self.schema = list(schema)
        self._validate()
        self.delimiter = delimiter
        self.skip_header_lines = skip_header_lines

    def __repr__(self):
        return repr(self.schema)

    def _schema_to_json(self):
        return [(field[0], supported_types.get_type_string(field[1]))
                for field in self.schema]

    @property
    def field_names(self):
        """
        List of field names from the schema stored in the CsvFile object.

        Returns
        -------
        list of string
            Field names
        
        Examples
        --------
        >>>
        For this example, we are going to use a raw data file called 'my_data.csv'. It will have two columns named 'col1' and 'col2' with types of int32 and float32 respectively.
        >>> my_csv = CsvFile("my_data.csv", [("col1", int32), ("col2", float32)])
        >>> print(my_csv.field_names)
        The output would be:
        ["col1", "col2"]
        """
        # TODO - Review docstring
        return [x[0] for x in self.schema]

    @property
    def field_types(self):
        """
        List of field types from the schema stored in the CsvFile object.
        
        Returns
        -------
        list of types
            Field types
        
        Examples
        --------
        >>>
        For this example, we are going to use a raw data file called 'my_data.csv'. It will have two columns named 'col1' and 'col2' with types of int32 and float32 respectively.
        >>> my_csv = CsvFile("my_data.csv", [("col1", int32), ("col2", float32)])
        >>> print(my_csv.field_types)
        The output would be:
        [numpy.int32, numpy.float32]
        """
        # TODO - Review docstring
        return [x[1] for x in self.schema]

    def _validate(self):
        for t in self.schema:
            if not isinstance(t[0], basestring):
                raise ValueError("First item in CSV schema tuple must be a string")
            if t[1] not in supported_types:
                raise ValueError("Second item in CSV schema tuple must be a supported type: " + str(supported_types))
