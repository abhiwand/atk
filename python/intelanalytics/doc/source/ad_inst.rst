Installation
============

**Yum Repo**

First add the epel RHEL/Centos
>>> wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
>>> rpm -ivh epel-release-6-8.noarch.rpm


*Inside Intel*
If you are working inside Intel you don't need any additional setup other than creating a repo file and picking the branch you would like to work out of.
The repo url is https://bda-internal-yum.s3-us-west-2.amazonaws.com/SomeBranch where SomeBranch is the name of the branch you'd like to track.
bda-internal-yum holds all the repos for branches built from Team City.
If your branch built successfully you should have a repo available for that branch.

Create a yum repo file ``/etc/yum.repos.d/bda-internal-yum.repo`` with the following contents:

>>> [bda-internal-yum]
    name=bda-internal-yum
    baseurl=https://bda-internal-yum.s3-us-west-2.amazonaws.com/mybranch
    gpgcheck=0
    priority=1
    enabled=1

Create a yum repo file ``/etc/yum.repos.d/bda-internal-yum-deps.repo`` with the following contents:

>>> [bda-internal-yum-deps]
    name=bda-internal-yum-deps
    baseurl=https://bda-public-repo.s3-us-west-2.amazonaws.com/yum
    gpgcheck=0
    priority=1
    enabled=1

*Outside Intel*
First add the dependencies repo and install yum-s3.

Create a yum repo file ``/etc/yum.repos.d/bda-internal-yum-deps.repo`` with the following contents:

>>> [bda-internal-yum-deps]
    name=bda-internal-yum-deps
    baseurl=https://bda-public-repo.s3-us-west-2.amazonaws.com/yum
    gpgcheck=0
    priority=1
    enabled=1

Then install the yum s3 plugin

>>> sudo yum install yum-s3

create a yum repo file ``/etc/yum.repos.d/bda-internal-yum.repo`` with the following contents:

>>> [bda-internal-yum]
    name=bda internal yum
    baseurl=https://bda-internal-yum.s3-us-west-2.amazonaws.com/mybranch
    gpgcheck=0
    priority=1
    s3_enabled=1
    #yum-get iam only has get
    key_id=AKIAJRVHQZHGTIVGBGJA
    secret_key=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

The security plug conflicts with the yum-s3 plugin so for now disable it by editing the security.conf file and setting enabled to 0.
The yum security plugin configuration can be found in ``/etc/yum/pluginconf.d/security.conf``

*Test*

>>> sudo yum search intelanalytics

Should return with two new packages intelanalytics-python-rest-client, intelanalytics-rest-server

>>> yum info

will return something like this

>>> sudo yum info intelanalytics-rest-server
    Available Packages
    Name: intelanalytics-rest-server
    Arch: x86_64
    Version: 0.8.212
    Release: 212
    Size: 134 M
    Repo: bda-internal-yum
    Summary: intelanalytics-rest-server-0.8.212 Build number: 212. TimeStamp 20140515062318Z
    URL: graphtrial.intel.com
    License: Confidential
Description : intelanalytics-rest-server-0.8.212 Build number: 212. TimeStamp 20140515062318Z start the server with 'service intelanalytics-rest-server status' config files are in /etc/intelanalytics/rest-server log files live in: /var/log/intelanalytics/rest-server commit 22524042160b51806650d9b0f7fea1b36c089967 Merge: 6f955fe 4e22864 Author: rodorad <rene.o.dorado@intel.com> Date: Wed May 14 23:11:05 2014 -0700 Merge branch 'package'

