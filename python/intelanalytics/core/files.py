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
from intelanalytics.core.iatypes import valid_data_types

class DataFile(object):
    annotation = "data_file"
    pass


class CsvFile(DataFile):
    """
    Define a CSV file.

    Parameters
    ----------
    file_name : string
        Name of data input file.
        File must be in the hadoop file system.
        Relative paths are interpreted relative to the intel.analytics.engine.fs.root configuration.
        Absolute paths (beginning with hdfs://..., for example) are also supported.
        See :ref:`Configure File System Root <ad_inst_IA_configure_file_system_root>`.
    schema : list of tuples of the form (string, type)
        schema description of the fields for a given line.
        It is a list of tuples which describe each field, (field name, field type),
        where the field name is a string, and file is a supported type,
        (See data_types from the iatypes module).
        The type ``ignore`` may also be used if the field should be ignored on loads.
    delimiter : string (optional)
        string indicator of the delimiter for the fields
    skip_header_lines : int (optional)
        indicates numbers of lines to skip before parsing records

    Raises
    ------
    ValueError
        *   "file_name must be a non-empty string": check for spurious leading comma in the parameters
        *   "schema must be non-empty list of tuples": check for spelling errors in the creation,
            building and application of the schema variable
        *   "delimiter must be a non-empty string": "" is not a valid delimiter between columns
        *   "First item in CSV schema tuple must be a string": check schema
        *   "Second item in CSV schema tuple must be a supported type: ...": check schema

    Examples
    --------
    For this example, we are going to use a raw data file named "raw_data.csv".
    The file has been moved to hdfs://localhost.localdomain/user/iauser/.
    It consists of three columns named: *a*, *b*, *c*.
    The columns have the data types: *int32*, *int32*, *str*.
    The fields of data are separated by commas.
    There is no header to the file.

    First bring in the stuff::

        import intelanalytics as ia

    At this point create a schema that defines the data::

        csv_schema = [("a", int32),
                      ("b", int32),
                      ("c", str)]

    Now build a CsvFile object with this schema::

        csv_define = ia.CsvFile("raw_data.csv", csv_schema)

    The standard delimiter in a csv file is the comma.
    If the columns of data were separated by a character other than comma, we need to add the appropriate delimiter.
    For example if the data columns were separated by the colon character, the instruction would be::

        csv_data = ia.CsvFile("raw_data.csv", csv_schema, ':')
            or
        ia.CsvFile("raw_data.csv", csv_schema, delimiter = ':')

    If our data had some lines of header at the beginning of the file, we could have skipped these lines::

        csv_data = ia.CsvFile("raw_data.csv", csv_schema, skip_header_lines=2)

    For other examples see :ref:`Importing a CSV File <example_files.csvfile>`.

    .. versionadded:: 0.8

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
        return [(field[0], valid_data_types.to_string(field[1]))
                for field in self.schema]

    @property
    def field_names(self):
        """
        Schema field names.

        List of field names from the schema stored in the CsvFile object

        Returns
        -------
        list of string
            Field names

        Examples
        --------
        For this example, we are going to use a raw data file called 'my_data.csv'.
        It will have two columns *col1* and *col2* with types of *int32* and *float32* respectively::

            my_csv = ia.CsvFile("my_data.csv", schema=[("col1", int32), ("col2", float32)])
            print(my_csv.field_names())

        The output would be::

            ["col1", "col2"]

        .. versionadded:: 0.8

        """
        # TODO - Review docstring
        return [x[0] for x in self.schema]

    @property
    def field_types(self):
        """
        Schema field types

        List of field types from the schema stored in the CsvFile object.

        Returns
        -------
        list of types
            Field types

        Examples
        --------
        For this example, we are going to use a raw data file called 'my_data.csv'.
        It will have two columns *col1* and *col2* with types of *int32* and *float32* respectively::

            my_csv = ia.CsvFile("my_data.csv", schema=[("col1", int32), ("col2", float32)])
            print(my_csv.field_types())

        The output would be::

            [numpy.int32, numpy.float32]

        .. versionadded:: 0.8

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

