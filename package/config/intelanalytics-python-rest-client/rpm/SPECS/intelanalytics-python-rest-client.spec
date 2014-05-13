Name: intelanalytics-python-rest-client
Provides: intelanalytics-python-rest-client
Summary: intelanalytics-python-rest-client-0.8.10 Build number: 10. TimeStamp 20140513185144Z
License: Confidential
Version: 0.8.10
Group: Intel Analytics
Requires: python2.7 python2.7-pip
Release: 10
Source: intelanalytics-python-rest-client-0.8.10.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-python-rest-client-0.8.10 Build number: 10. TimeStamp 20140513185144Z
commit bac0a2bee74a0dfa8865b497a2fce368fe5644d1 Author: rodorad <rene.o.dorado@intel.com> Date: Mon May 12 17:05:54 2014 -0700 add a pip requirements file to the python rest rpm
%define TIMESTAMP %(echo 20140513185144Z)
%define TAR_FILE %(echo /home/rodorad/IdeaProjects/source_code/package/intelanalytics-python-rest-client-source.tar.gz)
%build
 cp %{TAR_FILE} %{_builddir}/files.tar.gz
%install
 rm -rf %{buildroot}
 mkdir -p %{buildroot}
 mv files.tar.gz %{buildroot}/files.tar.gz
 tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}
 rm %{buildroot}/files.tar.gz
%clean
%post

 #sim link to python sites packages
 ln -s /usr/lib/intelanalytics/rest-client/python  /usr/lib/python2.7/site-packages/intelanalytics
 #run requirements file
 pip install -r /usr/lib/intelanalytics/rest-client/python/requirements.txt

%postun

 rm /usr/lib/python2.7/site-packages/intelanalytics

%files
/./usr/lib/intelanalytics/rest-client/python/rest/hooks.py
/./usr/lib/intelanalytics/rest-client/python/rest/message.py
/./usr/lib/intelanalytics/rest-client/python/rest/serialize.py
/./usr/lib/intelanalytics/rest-client/python/rest/graph.py
/./usr/lib/intelanalytics/rest-client/python/rest/spark.py
/./usr/lib/intelanalytics/rest-client/python/rest/frame.py
/./usr/lib/intelanalytics/rest-client/python/rest/connection.py
/./usr/lib/intelanalytics/rest-client/python/rest/prettytable.py
/./usr/lib/intelanalytics/rest-client/python/rest/launcher.py
/./usr/lib/intelanalytics/rest-client/python/rest/__init__.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_rest_row.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_little_frame.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_rest_api.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_serialize.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_rest_connection.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
/./usr/lib/intelanalytics/rest-client/python/tests/iatest.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
/./usr/lib/intelanalytics/rest-client/python/tests/.gitignore
/./usr/lib/intelanalytics/rest-client/python/tests/test_webhook.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
/./usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
/./usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
/./usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
/./usr/lib/intelanalytics/rest-client/python/tests/__init__.py
/./usr/lib/intelanalytics/rest-client/python/requirements.txt
/./usr/lib/intelanalytics/rest-client/python/little/frame.py
/./usr/lib/intelanalytics/rest-client/python/little/__init__.py
/./usr/lib/intelanalytics/rest-client/python/doc/source/rowfunc.rst
/./usr/lib/intelanalytics/rest-client/python/doc/source/index.rst
/./usr/lib/intelanalytics/rest-client/python/doc/source/_static/strike.css
/./usr/lib/intelanalytics/rest-client/python/doc/source/overapi.rst
/./usr/lib/intelanalytics/rest-client/python/doc/source/examples.rst
/./usr/lib/intelanalytics/rest-client/python/doc/source/conf.py
/./usr/lib/intelanalytics/rest-client/python/doc/Makefile
/./usr/lib/intelanalytics/rest-client/python/doc/__init__.py
/./usr/lib/intelanalytics/rest-client/python/core/files.py
/./usr/lib/intelanalytics/rest-client/python/core/serialize.py
/./usr/lib/intelanalytics/rest-client/python/core/graph.py
/./usr/lib/intelanalytics/rest-client/python/core/backend.py
/./usr/lib/intelanalytics/rest-client/python/core/loggers.py
/./usr/lib/intelanalytics/rest-client/python/core/column.py
/./usr/lib/intelanalytics/rest-client/python/core/config.py
/./usr/lib/intelanalytics/rest-client/python/core/frame.py
/./usr/lib/intelanalytics/rest-client/python/core/sources.py
/./usr/lib/intelanalytics/rest-client/python/core/row.py
/./usr/lib/intelanalytics/rest-client/python/core/__init__.py
/./usr/lib/intelanalytics/rest-client/python/core/types.py
/./usr/lib/intelanalytics/rest-client/python/__init__.py
