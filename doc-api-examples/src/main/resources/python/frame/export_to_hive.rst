Write current frame to a hive table.

Create a hive table and save the frame data to it.


Parameters
----------
tableName : str
    The name of the hive table to write the frame to. Creates the table in hive and will fail if the table already exists.


Examples
--------
Consider Frame *my_frame*:

.. code::

    >>> my_frame.export_to_hive('covarianceresults')