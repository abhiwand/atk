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
from collections import OrderedDict
from types import supported_types


class DataFile(object):
    annotation = "data_file"
    pass


class CsvFile(DataFile):
    """Describes a comma-separated file"""
    annotation = "csv_file"

    def __init__(self, file_name, schema, delimiter=',', skip_header_lines=0):
        """
        Creates object which defines a CSV file

        Parameters
        ----------

        file_name : str
            name of file
        schema : list of tuples of the form (str, type)
            schema description of the fields for a given line.  It is a list of
            tuples which describe each field, (field name, field type), where
            the field name is a string, and file is a supported type
            (See supported_types from the types module)
            The type 'ignore' may also be used if the field should be ignored
            on loads
        delimiter : str
            string indicator the delimiter for the fields, comma char is the default
        skip_header_lines : int, optional
            indicates numbers of lines to skip before parsing records

        Examples
        --------
        >>> csv1 = CsvFile("my_csv_data.txt", [('A', int32), ('B', string)])
        >>>
        >>> csv2 = CsvFile("other_data.txt", [('X', float32), ('', ignore), ('Y', int64)])
        >>> f = BigFrame(csv2)
        >>> csv2 = JsonFile("other_data.json", [('X', 'path/to/x', float32), ('', ignore), ('Y', int64)])
        """
        if not file_name  or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        if not schema:
            raise ValueError("schema must be non-empty list of tuples")
        if not delimiter or not isinstance(delimiter, basestring):
            raise ValueError("delimiter must be a non-empty string")
        self.file_name = file_name
        self.delimiter = delimiter
        self.skip_header_lines = skip_header_lines
        if isinstance(schema, basestring):
            self._init_from_string(schema)
        else:
            self._init_from_tuples(schema)
        self._validate()

    def __repr__(self):
        return repr(self.fields)

    def as_json_obj(self):
        return ["csv_file", self.file_name, self._schema_to_json(), self.delimiter, self.skip_header_lines]

    @classmethod
    def from_json_obj(cls, obj):
        assert("csv_file" == obj[0])
        obj = obj[1:]
        obj[1] = CsvFile._schema_from_json(obj[1])
        return CsvFile(*obj)

    def _schema_to_json(self):
        json_schema = []
        for f in self.fields:
           json_schema.append((f[0], supported_types.get_type_string(f[1])))
        return json_schema

    @staticmethod
    def _schema_from_json(schema_list):
        schema = []
        for f in schema_list:
           schema.append((f[0], supported_types.get_type_from_string(f[1])))
        return schema

    @property
    def field_names(self):
        """list of field names of the CSV file"""
        return [x[0] for x in self.fields]

    @property
    def field_types(self):
        """list of field types of the CSV file"""
        return [x[1] for x in self.fields]

    def to_ordered_dict(self):
        """creates an ordered dictionary representing the schema fields and types"""
        d = OrderedDict()
        for field in self.fields:
            d[field[0]] = field[1]
        return d

    def _init_from_tuples(self, tuples):
        self.fields = list(tuples)

    def _validate(self):
        for t in self.fields:
            if not isinstance(t[0], basestring):
                raise ValueError("First item in CSV schema tuple must be a string")
            if t[1] not in supported_types:
                raise ValueError("Second item in CSV schema tuple must be a supported type: " + str(supported_types))

    def _init_from_string(self, schema_string):
        """
        Parses the given csv schema string of the form:
           'x:chararray,y:double,z:int,l:long'
        """
        self.fields = CsvFile.parse_legacy_schema_string(schema_string)

    @staticmethod
    def parse_legacy_schema_string(schema_string):
        fields = []
        pairs = ("".join(schema_string.split())).split(',')
        for pair in pairs:
            fields.append(tuple(pair.split(':')))
        warnings.warn("CSV schema string format 'name:type' is deprecated",
                      DeprecationWarning)
        return fields


class JsonFile(DataFile):
    """Describes a JSON file"""
    annotation = "json_file"

    def __init__(self, file_name):
        """
        Creates object which defines a CSV file

        Parameters
        ----------

        file_name : str
            name of file
        """
        if not file_name or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        self.file_name = file_name


class XmlFile(DataFile):
    """Describes a XML file"""
    annotation = "xml_file"

    def __init__(self, file_name, tag_name=None):
        """
        Creates object which defines a XML file

        Parameters
        ----------
        file_name : str
            name of file
        tag_name : str, optional
            The XML tag name
        """
        if not file_name or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        self.file_name = file_name
        self.tag_name = tag_name
