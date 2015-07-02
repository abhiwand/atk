Name: intelanalytics-python-rest-client
Summary:  Branch: BLEH version: 0.4.0 Build number: 1000. TimeStamp Thu Jul  2 11:59:43 PDT 2015
License: Confidential
Version: 0.4.0
Group: Intel Analytics Client
Requires: python27, python27-ordereddict, python27-numpy >= 1.8.1, python27-bottle >= 0.12, python27-requests >= 2.2.1, python27-decorator >= 3.4.0, python27-pandas >= 0.15.0, python27-pymongo
Release: 1000
Source: intelanalytics-python-rest-client-0.4.0.tar.gz
URL: intel.com
%description
Branch: BLEH version: 0.4.0 Build number: 1000. TimeStamp Thu Jul 2 11:59:43 PDT 2015
%define TIMESTAMP %(echo Thu Jul  2 11:59:43 PDT 2015)
%define TAR_FILE %(echo /home/rodorad/source_code/serban2/package/tarballs/intelanalytics-python-rest-client/../intelanalytics-python-rest-client-source.tar.gz)
%build
 cp %{TAR_FILE} %{_builddir}/files.tar.gz
%install
 rm -rf %{buildroot}
 mkdir -p %{buildroot}
 mv files.tar.gz %{buildroot}/files.tar.gz
 tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}
 rm %{buildroot}/files.tar.gz
%clean
%pre
%post

 #sim link to python sites packages
 if [ -d /usr/lib/python2.7/site-packages/intelanalytics ]; then
   rm /usr/lib/python2.7/site-packages/intelanalytics
 fi

 ln -s /usr/lib/intelanalytics/rest-client/python  /usr/lib/python2.7/site-packages/intelanalytics

%preun
%postun

 if  [ $1 -eq 0 ]; then
    rm /usr/lib/python2.7/site-packages/intelanalytics
 fi

%files

/usr/lib/intelanalytics/rest-client
%config /usr/lib/intelanalytics/rest-client/python/rest/config.py

/usr/lib/intelanalytics/rest-client/python/requirements-linux.txt
/usr/lib/intelanalytics/rest-client/python/__init__.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_core_types.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_genrst.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/iatest.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
/usr/lib/intelanalytics/rest-client/python/tests/.gitignore
/usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
/usr/lib/intelanalytics/rest-client/python/tests/sources.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_jsonschema.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_metaprog.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.pyc
/usr/lib/intelanalytics/rest-client/python/tests/iatest.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_user_func.py
/usr/lib/intelanalytics/rest-client/python/tests/test_genrst.py
/usr/lib/intelanalytics/rest-client/python/tests/test_sources.pyc
/usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
/usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
/usr/lib/intelanalytics/rest-client/python/tests/__init__.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_core_files.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_command.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_core_metaprog.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_command.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_jsonschema.pyc
/usr/lib/intelanalytics/rest-client/python/tests/sources.pyc
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.pyc
/usr/lib/intelanalytics/rest-client/python/tests/__init__.py
/usr/lib/intelanalytics/rest-client/python/tests/test_user_func.pyc
/usr/lib/intelanalytics/rest-client/python/rest/config.py
/usr/lib/intelanalytics/rest-client/python/rest/udfdepends.pyc
/usr/lib/intelanalytics/rest-client/python/rest/progress.py
/usr/lib/intelanalytics/rest-client/python/rest/serializers.py
/usr/lib/intelanalytics/rest-client/python/rest/progress.pyc
/usr/lib/intelanalytics/rest-client/python/rest/http.py
/usr/lib/intelanalytics/rest-client/python/rest/jsonschema.py
/usr/lib/intelanalytics/rest-client/python/rest/udfdepends.py
/usr/lib/intelanalytics/rest-client/python/rest/jsonschema.pyc
/usr/lib/intelanalytics/rest-client/python/rest/server.py
/usr/lib/intelanalytics/rest-client/python/rest/command.py
/usr/lib/intelanalytics/rest-client/python/rest/iatypes.py
/usr/lib/intelanalytics/rest-client/python/rest/cloudpickle.pyc
/usr/lib/intelanalytics/rest-client/python/rest/frame.py
/usr/lib/intelanalytics/rest-client/python/rest/serializers.pyc
/usr/lib/intelanalytics/rest-client/python/rest/config.pyc
/usr/lib/intelanalytics/rest-client/python/rest/http.pyc
/usr/lib/intelanalytics/rest-client/python/rest/udfzip.py
/usr/lib/intelanalytics/rest-client/python/rest/__init__.pyc
/usr/lib/intelanalytics/rest-client/python/rest/udfzip.pyc
/usr/lib/intelanalytics/rest-client/python/rest/iaserver.pyc
/usr/lib/intelanalytics/rest-client/python/rest/graph.py
/usr/lib/intelanalytics/rest-client/python/rest/frame.pyc
/usr/lib/intelanalytics/rest-client/python/rest/iatypes.pyc
/usr/lib/intelanalytics/rest-client/python/rest/spark.py
/usr/lib/intelanalytics/rest-client/python/rest/server.pyc
/usr/lib/intelanalytics/rest-client/python/rest/uaa.pyc
/usr/lib/intelanalytics/rest-client/python/rest/command.pyc
/usr/lib/intelanalytics/rest-client/python/rest/__init__.py
/usr/lib/intelanalytics/rest-client/python/rest/prettytable.py
/usr/lib/intelanalytics/rest-client/python/rest/spark.pyc
/usr/lib/intelanalytics/rest-client/python/rest/uaa.py
/usr/lib/intelanalytics/rest-client/python/rest/iaserver.py
/usr/lib/intelanalytics/rest-client/python/rest/cloudpickle.py
/usr/lib/intelanalytics/rest-client/python/doc/python_api/graphs/.gitignore
/usr/lib/intelanalytics/rest-client/python/doc/python_api/models/.gitignore
/usr/lib/intelanalytics/rest-client/python/doc/python_api/frames/.gitignore
/usr/lib/intelanalytics/rest-client/python/doc/python_api/connect.rst
/usr/lib/intelanalytics/rest-client/python/doc/python_api/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/python_api/datatypes/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/python_api/datasources/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/restrst.py
/usr/lib/intelanalytics/rest-client/python/doc/pyrst.py
/usr/lib/intelanalytics/rest-client/python/doc/build_docs.py
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/entities/create_entity.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/entities/get_entity.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/entities/get_frame_data.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/entities/drop_entity.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/entities/get_named_entities.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/entities/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/info.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/commands/get_command.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/commands/issue_command.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/commands/command_full_name.png
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/commands/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/commands/about_command_names.rst
/usr/lib/intelanalytics/rest-client/python/doc/rest_api/v1/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/__init__.py
/usr/lib/intelanalytics/rest-client/python/core/files.py
/usr/lib/intelanalytics/rest-client/python/core/loggers.pyc
/usr/lib/intelanalytics/rest-client/python/core/iapandas.pyc
/usr/lib/intelanalytics/rest-client/python/core/histogram.py
/usr/lib/intelanalytics/rest-client/python/core/loggers.py
/usr/lib/intelanalytics/rest-client/python/core/admin.py
/usr/lib/intelanalytics/rest-client/python/core/iapandas.py
/usr/lib/intelanalytics/rest-client/python/core/row.pyc
/usr/lib/intelanalytics/rest-client/python/core/files.pyc
/usr/lib/intelanalytics/rest-client/python/core/row.py
/usr/lib/intelanalytics/rest-client/python/core/errorhandle.py
/usr/lib/intelanalytics/rest-client/python/core/api.py
/usr/lib/intelanalytics/rest-client/python/core/aggregation.py
/usr/lib/intelanalytics/rest-client/python/core/decorators.pyc
/usr/lib/intelanalytics/rest-client/python/core/iatypes.py
/usr/lib/intelanalytics/rest-client/python/core/frame.py
/usr/lib/intelanalytics/rest-client/python/core/api.pyc
/usr/lib/intelanalytics/rest-client/python/core/errorhandle.pyc
/usr/lib/intelanalytics/rest-client/python/core/clusteringcoefficient.py
/usr/lib/intelanalytics/rest-client/python/core/decorators.py
/usr/lib/intelanalytics/rest-client/python/core/__init__.pyc
/usr/lib/intelanalytics/rest-client/python/core/aggregation.pyc
/usr/lib/intelanalytics/rest-client/python/core/column.py
/usr/lib/intelanalytics/rest-client/python/core/column.pyc
/usr/lib/intelanalytics/rest-client/python/core/graph.pyc
/usr/lib/intelanalytics/rest-client/python/core/graph.py
/usr/lib/intelanalytics/rest-client/python/core/classifymetrics.py
/usr/lib/intelanalytics/rest-client/python/core/frame.pyc
/usr/lib/intelanalytics/rest-client/python/core/model.py
/usr/lib/intelanalytics/rest-client/python/core/iatypes.pyc
/usr/lib/intelanalytics/rest-client/python/core/__init__.py
/usr/lib/intelanalytics/rest-client/python/core/model.pyc
/usr/lib/intelanalytics/rest-client/python/__init__.py
/usr/lib/intelanalytics/rest-client/python/meta/namedobj.pyc
/usr/lib/intelanalytics/rest-client/python/meta/names.py
/usr/lib/intelanalytics/rest-client/python/meta/config.py
/usr/lib/intelanalytics/rest-client/python/meta/npdoc.py
/usr/lib/intelanalytics/rest-client/python/meta/reflect.pyc
/usr/lib/intelanalytics/rest-client/python/meta/metaprog.pyc
/usr/lib/intelanalytics/rest-client/python/meta/clientside.py
/usr/lib/intelanalytics/rest-client/python/meta/names.pyc
/usr/lib/intelanalytics/rest-client/python/meta/installapi.py
/usr/lib/intelanalytics/rest-client/python/meta/udf.pyc
/usr/lib/intelanalytics/rest-client/python/meta/installpath.py
/usr/lib/intelanalytics/rest-client/python/meta/command.py
/usr/lib/intelanalytics/rest-client/python/meta/context.pyc
/usr/lib/intelanalytics/rest-client/python/meta/spa.pyc
/usr/lib/intelanalytics/rest-client/python/meta/config.pyc
/usr/lib/intelanalytics/rest-client/python/meta/mute.py
/usr/lib/intelanalytics/rest-client/python/meta/docstub.pyc
/usr/lib/intelanalytics/rest-client/python/meta/installapi.pyc
/usr/lib/intelanalytics/rest-client/python/meta/mute.pyc
/usr/lib/intelanalytics/rest-client/python/meta/spa.py
/usr/lib/intelanalytics/rest-client/python/meta/reflect.py
/usr/lib/intelanalytics/rest-client/python/meta/installpath.pyc
/usr/lib/intelanalytics/rest-client/python/meta/clientside.pyc
/usr/lib/intelanalytics/rest-client/python/meta/__init__.pyc
/usr/lib/intelanalytics/rest-client/python/meta/udf.py
/usr/lib/intelanalytics/rest-client/python/meta/docstub.py
/usr/lib/intelanalytics/rest-client/python/meta/metaprog.py
/usr/lib/intelanalytics/rest-client/python/meta/namedobj.py
/usr/lib/intelanalytics/rest-client/python/meta/command.pyc
/usr/lib/intelanalytics/rest-client/python/meta/results.py
/usr/lib/intelanalytics/rest-client/python/meta/__init__.py
/usr/lib/intelanalytics/rest-client/python/meta/context.py
/intelanalytics-python-rest-client/EULA.html
