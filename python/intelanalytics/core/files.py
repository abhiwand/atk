##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
from intelanalytics.core.iatypes import valid_data_types

class DataFile(object):
    annotation = "data_file"
    pass


class CsvFile(DataFile):
    """
    Define a CSV file.

    Parameters
    ----------
    file_name : str
        Name of data input file.
        File must be in the Hadoop file system.
        Relative paths are interpreted relative to the path set in
        ``intel.analytics.engine.fs.root`` configuration.
        Absolute paths (beginning with ``hdfs://...``, for example) are also
        supported.
        See :ref:`Configure File System Root
        <ad_inst_IA_configure_file_system_root>`.
    schema : list of tuples of the form (string, type)
        Description of the fields of data.
        It is a list of tuples which describe each field, (field name, field
        type), where the field name is a string, and file is a supported type,
        (See data_types from the iatypes module).
        Unicode characters should not be used in the column name.
        The type ``ignore`` may also be used if the field should be ignored on loads.
    delimiter : str (optional)
        String indicator of the delimiter for the fields
    skip_header_lines : int (optional)
        Indicates numbers of lines to skip before parsing records.

    Returns
    -------
    class : CsvFile object
        A class which holds both the name and schema of a :term:`CSV` file.

    Examples
    --------
    Given a raw data file named "raw_data.csv", located at
    ``hdfs://localhost.localdomain/user/iauser/data/``.
    It consists of three columns, *a*, *b*, and *c*.
    The columns have the data types *int32*, *int32*, and *str* respectively.
    The fields of data are separated by commas.
    There is no header to the file.

    Import the |IAT|::

        import intelanalytics as ia

    Define the data::

        csv_schema = [("a", int32),
                      ("b", int32),
                      ("c", str)]

    Create a CsvFile object with this schema::

        csv_define = ia.CsvFile("data/raw_data.csv", csv_schema)

    The default delimiter, a comma, was used to separate fields in the file, so
    it was not specified.
    If the columns of data were separated by a character other than comma, the
    appropriate delimiter would be specified.
    For example if the data columns were separated by the colon character, the
    instruction would be::

        ia.CsvFile("data/raw_data.csv", csv_schema, delimiter = ':')

    If the data had some lines of header at the beginning of the file, the
    lines should be skipped::

        csv_data = ia.CsvFile("data/raw_data.csv", csv_schema, skip_header_lines=2)

    For other examples see :ref:`Importing a CSV File <example_files.csvfile>`.

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
        return repr(self.__dict__)

    def _schema_to_json(self):
        return [(field[0], valid_data_types.to_string(field[1]))
                for field in self.schema]

    @property
    def field_names(self):
        """
        |BETA| Schema field names from the CsvFile class.

        |TB|

        Returns
        -------
        list : list of str
            Field names

        Examples
        --------
        Given a raw data file ``raw_data.csv`` with columns *col1* (*int32*)
        and *col2* (*float32*)::

            csv_class = ia.CsvFile("raw_data.csv", schema=[("col1", int32), ("col2", float32)])
            print(csv_class.field_names())

        Results::

            ["col1", "col2"]

        """
        # TODO - Review docstring
        return [x[0] for x in self.schema]

    @property
    def field_types(self):
        """
        Schema field types from the CsvFile class.

        Returns
        -------
        list : list of types
            Field types

        Examples
        --------
        Given a raw data file ``raw_data.csv`` with columns *col1* (*int32*)
        and *col2* (*float32*)::

            csv_class = ia.CsvFile("raw_data.csv", schema=[("col1", int32), ("col2", float32)])
            print(csv_class.field_types())

        Results::

            [numpy.int32, numpy.float32]

        """
        # TODO - Review docstring
        return [x[1] for x in self.schema]

    def _validate(self):
        validated_schema = []
        for field in self.schema:
            name = field[0]
            if not isinstance(name, basestring):
                raise ValueError("First item in CSV schema tuple must be a string")
            try:
                data_type = valid_data_types.get_from_type(field[1])
            except ValueError:
                raise ValueError("Second item in CSV schema tuple must be a supported type: " + str(valid_data_types))
            else:
                validated_schema.append((name, data_type))
        self.schema = validated_schema

class LineFile(DataFile):
    """
    Define a line-separated file.

    Parameters
    ----------
    file_name : str
        Name of data input file.
        File must be in the Hadoop file system.
        Relative paths are interpreted relative to the
        ``intel.analytics.engine.fs.root`` configuration.
        Absolute paths (beginning with ``hdfs://...``, for example) are also
        supported.
        See :ref:`Configure File System Root
        <ad_inst_IA_configure_file_system_root>`.

    Returns
    -------
    class : LineFile object
        A class which holds the name of a :term:`Line` file.

    Examples
    --------
    Given a raw data file ``rawline_data.txt`` located at
    ``hdfs://localhost.localdomain/user/iauser/data/``.
    It consists of multiple lines separated by new line character.

    Import the |IAT|::

        import intelanalytics as ia
        ia.connect()

    Define the data::

        linefile_class = ia.LineFile("data/rawline_data.txt")

    """

    # TODO - Review docstring
    annotation = "line_file"

    def __init__(self, file_name):
        if not file_name or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        self.file_name = file_name

    def __repr__(self):
        return repr(self.file_name)

class MultiLineFile(DataFile):

    annotation = "multline_file"

    def __init__(self, file_name, start_tag=None, end_tag=None):
        if not file_name or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        self.file_name = file_name
        self.start_tag = start_tag
        self.end_tag = end_tag

    def __repr__(self):
        return repr(self.file_name)


class JsonFile(MultiLineFile):
    """
    Define a file as having data in JSON format.

    When JSON files are loaded into the system all top level JSON objects are
    recorded into the frame as seperate elements.

    Parameters
    ----------
    file_name : str
        Name of data input file.
        File must be in the Hadoop file system.
        Relative paths are interpreted relative to the
        ``intel.analytics.engine.fs.root`` configuration.
        Absolute paths (beginning with ``hdfs://...``, for example) are also
        supported.
        See :ref:`Configure File System Root
        <ad_inst_IA_configure_file_system_root>`.

    Returns
    -------
    class : JsonFile object
        An object which holds both the name and tag of a :term:`JSON` file.

    Examples
    --------
    Give a raw data file named "raw_data.json" located at
    hdfs://localhost.localdomain/user/iauser/data/.
    It consists of a 3 top level json objects with a single value each called
    obj. Each object contains the attributes color, size, and shape.

    The example JSON file::
        { "obj": {
            "color": "blue",
            "size": 3,
            "shape": "square" }
        }
        { "obj": {
            "color": "green",
            "size": 7,
            "shape": "triangle" }
        }
        { "obj": {
            "color": "orange",
            "size": 10,
            "shape": "square" }
        }

    Import the |IAT|::

        import intelanalytics as ia
        ia.connect()

    Define the data::

        json_file = ia.JsonFile("data/raw_data.json")

    Create a frame using this JsonFile::

        f = ia.Frame(json_file)

    The frame looks like::

         data_lines
         ------------------------
        '{ "obj": {
            "color": "blue",
            "size": 3,
            "shape": "square" }
        }'
        '{ "obj": {
            "color": "green",
            "size": 7,
            "shape": "triangle" }
        }'
        '{ "obj": {
            "color": "orange",
            "size": 10,
            "shape": "square" }
        }'

    Parse values out of the XML column using the add_columns method::

        def parse_my_json(row):
            import json
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return (obj['color'], obj['size'], obj['shape'])

        f.add_columns(parse_my_json, [("color", str), ("size", str), ("shape", str)])

    Original XML column is no longer necessary::

        f.drop_columns(['data_lines'])

    Result::

        f.inspect()

          color:str   size:str    shape:str
        /-----------------------------------/
          blue        3           square
          green       7           triangle
          orange      10          square

    """

    annotation = "json_file"

    def __init__(self, file_name):
        if not file_name or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        MultiLineFile.__init__(self, file_name, ['{'], ['}'])

    def __repr__(self):
        return repr(self.file_name)


class XmlFile(MultiLineFile):
    """
    Define an file as having data in XML format.
    
    When XML files are loaded into the system individual records are separated
    into the highest level elements found with the specified tag name and
    places them into a column called data_lines.

    Parameters
    ----------
    file_name : str
        Name of data input file.
        File must be in the Hadoop file system.
        Relative paths are interpreted relative to the
        ``intel.analytics.engine.fs.root`` configuration.
        Absolute paths (beginning with ``hdfs://...``, for example) are also
        supported.
        See :ref:`Configure File System Root
        <ad_inst_IA_configure_file_system_root>`.
    tag_name : str
        Tag name used to determine the split of elements into separate records.

    Returns
    -------
    class : XmlFile object
        An object which holds both the name and tag of a :term:`XML` file.

    Examples
    --------
    Given a raw data file named "raw_data.xml" located at
    ``hdfs://localhost.localdomain/user/iauser/data/``.
    It consists of a root element called shapes with 2 sub elements with the
    tag name square.
    Each of these subelements has two subelements called name and size.
    One of the elements has an attribute called color.
    Additionally there is one triangle element that is not needed.

    The example XML file::

        <?xml version="1.0" encoding="UTF-8"?>
        <shapes>
            <square>
                <name>left</name>
                <size>3</size>
            </square>
            <triangle>
                <size>3</size>
            </triangle>
            <square color="blue">
                <name>right</name>
                <size>5</size>
            </square>
        </shapes>

    Import the |IAT|::

        import intelanalytics as ia
        ia.connect()

    Define the data::

        xml_file = ia.XmlFile("data/raw_data.xml", "square")

    Create a frame using this XmlFile::

        f = ia.Frame(xml_file)

    The frame looks like::

         data_lines
         ------------------------
         '<square>
                <name>left</name>
                <size>3</size>
            </square>'
         '<square color="blue">
                <name>right</name>
                <size>5</size>
            </square>'

    Parse values out of the XML column using the add_columns method::

        def parse_my_xml(row):
            import xml.etree.ElementTree as ET
            ele = ET.fromstring(row[0])
            return (ele.get("color"), ele.find("name").text, ele.find("size").text)

        f.add_columns(parse_my_xml, [("color", str), ("name", str), ("size", str)])

    Original XML column is no longer necessary::

        f.drop_columns(['data_lines'])

    Result::

        f.inspect()

          color:str   name:str    size:str
        /----------------------------------/
          None        left        3
          blue        right       5


    """

    annotation = "xml_file"

    def __init__(self, file_name, tag_name):
        if not file_name or not isinstance(file_name, basestring):
            raise ValueError("file_name must be a non-empty string")
        if not tag_name or not isinstance(tag_name, basestring):
            raise ValueError("tag_name is required")
        MultiLineFile.__init__(self, file_name, ['<%s>' % tag_name, '<%s ' % tag_name], ['</%s>' % tag_name])

    def __repr__(self):
        return repr(self.file_name)

