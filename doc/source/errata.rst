======
Errata
======

2014-11-20

*   Quick search:

    *   When using the documentation search feature with certain web browsers, there are certain combinations of letters, which if entered in the search box will cause the screen to appear to hang with ellipses building after the word "Searching".
        The most common incident occurs if the search term is three characters long and could be recognized by a dictionary as a normal word.
        The search function does not "hang" or freeze the window, but it will not return any results.
        It will still work for other searches.
    *   Search is insensitive to plurals and capitalization.
        For example, a search of "host" will find "Hosts" and a search of "Installs" will find "install".
    *   Search will not find partitial words.
        For example, a search of "host" will not find "hostname".

2014-10-08:


*   Frame column name can accept unicode characters, but it should be avoided because some functions such
    as delete_column will fail.

*   Renaming a graph to a name containing one or more of the special characters \@\#\$\%\^\&\* will
    cause the application to hang for a long time and then raise an error.

*   Attempting to create a frame with a parenthesis in the name will raise the error::

        intelanalytics.rest.command.CommandServerError: Job aborted due to stage failure:
        Task 7.0:5 failed 4 times, most recent failure: Exception failure in TID 426 on host
        node03.zonda.cluster: java.lang.IllegalArgumentException: No enum constant parquet
        .schema.OriginalType.

*   Creating a table with an invalid source data file name causes the server to return an error message
    and abort, but also creates the empty (named) frame.

