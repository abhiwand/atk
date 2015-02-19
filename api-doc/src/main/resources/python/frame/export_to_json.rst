Write frame to HDFS in json format.

Export the frame to a file in json format as a Hadoop file.

Parameters
----------

folderName : string
    The HDFS folder path where the files will be created

separator : string (optional)
    The separator for separating the values.
    Default is ",".

count : int (optional)
    The number of records you want.
    Default, or value less than 1, is the whole frame.

offset : int (optional)
    The number of rows to skip before exporting to the file.
    Default is zero (0).

Returns
-------
None

Examples
--------
Consider Frame *my_frame*

    my_frame.export_to_json('covarianceresults')

