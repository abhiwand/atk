Name: intelanalytics-python-rest-client
Summary:  Branch: master version: 1.1 Build number: 6645. TimeStamp Tue May 12 11:16:51 PDT 2015
License: Confidential
Version: 1.1
Group: Intel Analytics Client
Requires: python27, python27-ordereddict, python27-numpy >= 1.8.1, python27-bottle >= 0.12, python27-requests >= 2.2.1, python27-decorator >= 3.4.0, python27-pandas >= 0.15.0, python27-pymongo
Release: 6645
Source: intelanalytics-python-rest-client-1.1.tar.gz
URL: intel.com
%description
Branch: master version: 1.1 Build number: 6645. TimeStamp Tue May 12 11:16:51 PDT 2015
%define TIMESTAMP %(echo Tue May 12 11:16:51 PDT 2015)
%define TAR_FILE %(echo /home/rodorad/source_code/source_code/package/tarballs/intelanalytics-python-rest-client/../intelanalytics-python-rest-client-source.tar.gz)
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

/usr/lib/intelanalytics/rest-client/cmdgen.py
/usr/lib/intelanalytics/rest-client/python/requirements-linux.txt
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_graph.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/iatest.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
/usr/lib/intelanalytics/rest-client/python/tests/.gitignore
/usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
/usr/lib/intelanalytics/rest-client/python/tests/test_namedobj.py
/usr/lib/intelanalytics/rest-client/python/tests/sources.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_jsonschema.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_connection.py
/usr/lib/intelanalytics/rest-client/python/tests/test_user_func.py
/usr/lib/intelanalytics/rest-client/python/tests/test_genrst.py
/usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
/usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
/usr/lib/intelanalytics/rest-client/python/tests/test_serialize.py
/usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
/usr/lib/intelanalytics/rest-client/python/tests/test_webhook.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_metaprog.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_command.py
/usr/lib/intelanalytics/rest-client/python/tests/__init__.py
/usr/lib/intelanalytics/rest-client/python/rest/config.py
/usr/lib/intelanalytics/rest-client/python/rest/serializers.py
/usr/lib/intelanalytics/rest-client/python/rest/message.py
/usr/lib/intelanalytics/rest-client/python/rest/http.py
/usr/lib/intelanalytics/rest-client/python/rest/jsonschema.py
/usr/lib/intelanalytics/rest-client/python/rest/udfdepends.py
/usr/lib/intelanalytics/rest-client/python/rest/server.py
/usr/lib/intelanalytics/rest-client/python/rest/command.py
/usr/lib/intelanalytics/rest-client/python/rest/iatypes.py
/usr/lib/intelanalytics/rest-client/python/rest/frame.py
/usr/lib/intelanalytics/rest-client/python/rest/udfzip.py
/usr/lib/intelanalytics/rest-client/python/rest/hooks.py
/usr/lib/intelanalytics/rest-client/python/rest/launcher.py
/usr/lib/intelanalytics/rest-client/python/rest/graph.py
/usr/lib/intelanalytics/rest-client/python/rest/spark.py
/usr/lib/intelanalytics/rest-client/python/rest/__init__.py
/usr/lib/intelanalytics/rest-client/python/rest/prettytable.py
/usr/lib/intelanalytics/rest-client/python/rest/uaa.py
/usr/lib/intelanalytics/rest-client/python/rest/iaserver.py
/usr/lib/intelanalytics/rest-client/python/rest/cloudpickle.py
/usr/lib/intelanalytics/rest-client/python/doc/api_template/graphs/.gitignore
/usr/lib/intelanalytics/rest-client/python/doc/api_template/models/.gitignore
/usr/lib/intelanalytics/rest-client/python/doc/api_template/build_rst.py
/usr/lib/intelanalytics/rest-client/python/doc/api_template/frames/.gitignore
/usr/lib/intelanalytics/rest-client/python/doc/api_template/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/api_template/datatypes/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/api_template/datasources/index.rst
/usr/lib/intelanalytics/rest-client/python/core/files.py
/usr/lib/intelanalytics/rest-client/python/core/histogram.py
/usr/lib/intelanalytics/rest-client/python/core/loggers.py
/usr/lib/intelanalytics/rest-client/python/core/iapandas.py
/usr/lib/intelanalytics/rest-client/python/core/row.py
/usr/lib/intelanalytics/rest-client/python/core/errorhandle.py
/usr/lib/intelanalytics/rest-client/python/core/api.py
/usr/lib/intelanalytics/rest-client/python/core/aggregation.py
/usr/lib/intelanalytics/rest-client/python/core/iatypes.py
/usr/lib/intelanalytics/rest-client/python/core/frame.py
/usr/lib/intelanalytics/rest-client/python/core/deprecate.py
/usr/lib/intelanalytics/rest-client/python/core/clusteringcoefficient.py
/usr/lib/intelanalytics/rest-client/python/core/decorators.py
/usr/lib/intelanalytics/rest-client/python/core/column.py
/usr/lib/intelanalytics/rest-client/python/core/graph.py
/usr/lib/intelanalytics/rest-client/python/core/classifymetrics.py
/usr/lib/intelanalytics/rest-client/python/core/model.py
/usr/lib/intelanalytics/rest-client/python/core/__init__.py
/usr/lib/intelanalytics/rest-client/python/__init__.py
/usr/lib/intelanalytics/rest-client/python/meta/config.py
/usr/lib/intelanalytics/rest-client/python/meta/npdoc.py
/usr/lib/intelanalytics/rest-client/python/meta/clientside.py
/usr/lib/intelanalytics/rest-client/python/meta/metaprog2.py
/usr/lib/intelanalytics/rest-client/python/meta/installpath.py
/usr/lib/intelanalytics/rest-client/python/meta/api.py
/usr/lib/intelanalytics/rest-client/python/meta/command.py
/usr/lib/intelanalytics/rest-client/python/meta/classnames.py
/usr/lib/intelanalytics/rest-client/python/meta/genspa.py
/usr/lib/intelanalytics/rest-client/python/meta/mute.py
/usr/lib/intelanalytics/rest-client/python/meta/genrst.py
/usr/lib/intelanalytics/rest-client/python/meta/reflect.py
/usr/lib/intelanalytics/rest-client/python/meta/udf.py
/usr/lib/intelanalytics/rest-client/python/meta/serialize.py
/usr/lib/intelanalytics/rest-client/python/meta/namedobj.py
/usr/lib/intelanalytics/rest-client/python/meta/gendoc.py
/usr/lib/intelanalytics/rest-client/python/meta/results.py
/usr/lib/intelanalytics/rest-client/python/meta/__init__.py
/usr/lib/intelanalytics/rest-client/python/meta/context.py
/intelanalytics-python-rest-client/EULA.html
