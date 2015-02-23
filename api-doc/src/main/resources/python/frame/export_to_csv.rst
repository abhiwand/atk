Write current frame to HDFS in csv format.

Export the frame to a file in csv format as a Hadoop file.

Parameters
----------

folderName : str
    The HDFS folder path where the files will be created

separator : str (optional)
    The separator for separating the values.
    Default is comma (,)

count :nt (optional) 
    The number of records you want.
    Default, or a non-positive value, is the whole frame.

offset : int (optional)
    The number of rows to skip before exporting to the file.
    Default is zero (0).


Returns
-------
None

Examples
--------
Consider Frame *my_frame*

    my_frame.export_to_csv('covarianceresults')

