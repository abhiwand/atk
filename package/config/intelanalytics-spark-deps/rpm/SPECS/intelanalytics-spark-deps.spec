Name: intelanalytics-spark-deps
Summary: intelanalytics-spark-deps-0.8 Build number: 901. TimeStamp 20140624195913Z
License: Confidential
Version: 0.8
Group: Intel Analytics
Requires: java >= 1.7
Release: 901
Source: intelanalytics-spark-deps-0.8.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-spark-deps-0.8 Build number: 901. TimeStamp 20140624195913Z
commit 06aee68eee188e14b34c781b828307450130f5d0 Merge: 5db0b28 2165283 Author: rodorad <rene.o.dorado@intel.com> Date: Tue Jun 24 12:04:01 2014 -0700 Merge remote-tracking branch 'origin/package' into sprint_14_package
%define TIMESTAMP %(echo 20140624195913Z)
%define TAR_FILE %(echo /home/rodorad/IdeaProjects/intellij/source_code/package/intelanalytics-spark-deps-source.tar.gz)
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



%preun
%postun



%files

/usr/lib/intelanalytics/graphbuilder/lib

/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar
