Name: intelanalytics-python-rest-client
Provides: intelanalytics-python-rest-client
Summary: intelanalytics-python-rest-client-0.8.0 Build number: 12345. TimeStamp 20140429232110Z
License: Apache
Version: 0.8.0
Group: Intel Analytics
Release: 12345
Source: intelanalytics-python-rest-client-0.8.0.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-python-rest-client-0.8.0 Build number: 12345. TimeStamp 20140429232110Z
%define TIMESTAMP %(echo 20140429232110Z)
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
%postun
%files
/usr/lib/intelanalytics/rest-client/python/rest/serialize.py
/usr/lib/intelanalytics/rest-client/python/rest/frame.py
/usr/lib/intelanalytics/rest-client/python/rest/connection.py
/usr/lib/intelanalytics/rest-client/python/rest/__init__.py
/usr/lib/intelanalytics/rest-client/python/tests/test_little_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_api.py
/usr/lib/intelanalytics/rest-client/python/tests/test_serialize.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_connection.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/iatest.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
/usr/lib/intelanalytics/rest-client/python/tests/.gitignore
/usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
/usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
/usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
/usr/lib/intelanalytics/rest-client/python/tests/__init__.py
/usr/lib/intelanalytics/rest-client/python/little/frame.py
/usr/lib/intelanalytics/rest-client/python/little/__init__.py
/usr/lib/intelanalytics/rest-client/python/doc/source/rowfunc.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/_static/strike.css
/usr/lib/intelanalytics/rest-client/python/doc/source/overapi.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/examples.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/conf.py
/usr/lib/intelanalytics/rest-client/python/doc/Makefile
/usr/lib/intelanalytics/rest-client/python/doc/__init__.py
/usr/lib/intelanalytics/rest-client/python/core/files.py
/usr/lib/intelanalytics/rest-client/python/core/serialize.py
/usr/lib/intelanalytics/rest-client/python/core/graph.py
/usr/lib/intelanalytics/rest-client/python/core/backend.py
/usr/lib/intelanalytics/rest-client/python/core/column.py
/usr/lib/intelanalytics/rest-client/python/core/config.py
/usr/lib/intelanalytics/rest-client/python/core/frame.py
/usr/lib/intelanalytics/rest-client/python/core/sources.py
/usr/lib/intelanalytics/rest-client/python/core/row.py
/usr/lib/intelanalytics/rest-client/python/core/__init__.py
/usr/lib/intelanalytics/rest-client/python/core/types.py
/usr/lib/intelanalytics/rest-client/python/__init__.py
