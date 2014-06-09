Python API in Detail
====================

Files
-----
.. automodule:: intelanalytics.core.files
    :members:

Frame
-----
.. automodule:: intelanalytics.core.frame
    :members:

.. toctree::

.. py:class:: FrameSchema(source=None)

    Ordered key-value pairs of column name -> data type.

    Parameters
    ----------

    source : str or tuple
        What to use as the pattern for the schema

    Raises
    ------

    ValueError
        "Unsupported data type in schema"

    Examples
    --------

    >>>

.. TODO:: add example

\ 

Methods
~~~~~~~

=============================== ==================================================
append(new_columns)             Add new columns.
drop(victim_column_names)       Remove particular columns.
get_column_data_type_strings()  Extract the column types as strings from a schema.
get_column_data_types()         Extract the column types from a schema.
get_column_names()              Extract the column names from a schema.
merge(schema)                   Merge two schemas.
=============================== ==================================================

.. py:method:: append(new_columns)

        Add new columns.

Parameters
----------

        new_columns : tuple (key : value)
            The column(s) to add
            key : string    The new column name
            value : type    The new column data type
            
Raises
------

        KeyError

Examples
--------

        >>> my_schema = FrameSchema()
        >>> my_schema.append([ "Twelve", string ])
        my_schema now contains the schema for an additional string column called Twelve


Graph
-----
.. automodule:: intelanalytics.core.graph
    :members:
