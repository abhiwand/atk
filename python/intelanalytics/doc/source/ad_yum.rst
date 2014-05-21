Yum Repo
========

First add the epel RHEL/Centos

>>> wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
    sudo rpm -ivh epel-release-6-8.noarch.rpm

Now build a yum repo file ``/etc/yum.repos.d/bda-internal-yum-deps.repo`` with the following contents:

>>> [bda-internal-yum-deps]
    name=bda-internal-yum-deps
    baseurl=https://bda-public-repo.s3-us-west-2.amazonaws.com/yum
    gpgcheck=0
    priority=1
    enabled=1

.. ifconfig:: internal_docs

    Inside Intel
    ------------
    If you are working inside Intel you need to create another repo file which will include the branch you would like to work out of.
    The repo url is https://bda-internal-yum.s3-us-west-2.amazonaws.com/MY-Branch where My-Branch is the name of the branch you'd like to track.
    bda-internal-yum holds all the repos for branches built from Team City.
    If your branch built successfully you should have a repo available for that branch.

    | Create a yum repo file ``/etc/yum.repos.d/bda-internal-yum.repo`` with the following contents:
    |    **Change "My-Branch" to the name of the real branch!**

    >>> [bda-internal-yum]
        name=bda-internal-yum
        baseurl=https://bda-internal-yum.s3-us-west-2.amazonaws.com/My-Branch
        gpgcheck=0
        priority=1
        enabled=1


    Outside Intel
    -------------

Install the yum-s3 plugin

>>> sudo yum install yum-s3

Create a yum repo file ``/etc/yum.repos.d/bda-internal-yum.repo`` with the following contents:

>>> [bda-internal-yum]
    name=bda internal yum
    baseurl=https://bda-internal-yum.s3-us-west-2.amazonaws.com/TRIBECA_PRODUCTION_SOURCE
    gpgcheck=0
    priority=1
    s3_enabled=1
    #yum-get iam only has get
    key_id=AKIAJRVHQZHGTIVGBGJA
    secret_key=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

The normal yum security plugin conflicts with the yum-s3 plugin so, for now, disable it.
The yum security plugin configuration can be found in ``/etc/yum/pluginconf.d/security.conf``.
Edit the file and change the "enabled" value to 0.

Test
----

>>> sudo yum search intelanalytics

Should return with two new packages ``intelanalytics-python-rest-client`` and ``intelanalytics-rest-server``

>>> yum info intelanalytics*

will return something like this

>>> Name        : intelanalytics-python-rest-client
    Arch        : x86_64
    Version     : 0.8.292
    Release     : 292
    Size        : 795 k
    Repo        : installed
    From repo   : bda-internal-yum
    Summary     : intelanalytics-python-rest-client-0.8
    URL         : graphtrial.intel.com
    License     : Confidential
    Description : intelanalytics-python-rest-client-0.8

>>> Name        : intelanalytics-rest-server
    Arch        : x86_64
    Version     : 0.8.292
    Release     : 292
    Size        : 150 M
    Repo        : installed
    From repo   : bda-internal-yum
    Summary     : intelanalytics-rest-server-0.8
    URL         : graphtrial.intel.com
    License     : Confidential
    Description : intelanalytics-rest-server-0.8

Install the Intel® Analytics package

>>> sudo yum install intelanalytics*

Congratulations, you have installed the Intel® Analytics package.
