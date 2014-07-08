=======================================
Apt-get Repo (Ubuntu & Similar Systems)
=======================================

.. ifconfig:: internal_docs

    ------------
    Inside Intel
    ------------

    Add the following line to your '/etc/apt/source.list' file:

    >>> deb https://bda-internal-apt.s3-us-west-2.amazonaws.com/MY-PACKAGE dev main

    Then::

        sudo apt-get update
        apt-cache search intelanalytics

    To install::

        sudo apt-cache install intelanalytics-rest-server
        sudo apt-cache install intelanalytics-python-rest-client

    -------------
    Outside Intel
    -------------

First, to add the public apt depency repo so you can install the s3 apt
plugin, add the following line to the file '/etc/apt/source.list':

>>> deb https://bda-public-repo.s3-us-west-2.amazonaws.com/apt deps main

then run::

    sudo apt-get install apt-transport-s3

After installing the S3 apt plugin add this line to the file '/etc/apt/source.list':

>>> deb s3://AKIAJVL3X4RZQK2ZHEPA:[B9PMgRaL+IDHOIajyJcXd2y70b5gcjw5LcEdY4xi]@bda-internal-apt.s3-us-west-2.amazonaws.com/MY-PACKAGE dev main

Then run::

    sudo apt-get update
    apt-cache search intelanalytics

To install the Intel Analytics package, run::

    sudo apt-cache install intelanalytics-rest-server

Congratulations, you have installed the Intel® Analytics package.

------------------
Installing Patches
------------------

To install patches, simply update the packages::

    sudo apt-get update

