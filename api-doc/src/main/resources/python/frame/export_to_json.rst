Write current frame to HDFS in JSON format.

Export the frame to a file in JSON format as a Hadoop file.

Parameters
----------

folderName : str
    The HDFS folder path where the files will be created
count : int (optional)
    The number of records you want.
    Default, or value less than 1, is the whole frame.
offset : int (optional)
    The number of rows to skip before exporting to the file.
    Default is zero (0).

Examples
--------
Consider Frame *my_frame*:

.. code::

    >>> my_frame.export_to_json('covarianceresults')

