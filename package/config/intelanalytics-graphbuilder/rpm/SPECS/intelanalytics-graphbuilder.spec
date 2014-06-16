Name: intelanalytics-graphbuilder
Provides: intelanalytics-graphbuilder
Summary: intelanalytics-graphbuilder-0.8.671 Build number: 671. TimeStamp 20140611181230Z
License: Confidential
Version: 0.8.671
Group: Intel Analytics
Requires: java >= 1.7, intelanalytics-spark-deps >= 0.8.671-671
Release: 671
Source: intelanalytics-graphbuilder-0.8.671.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-graphbuilder-0.8.671 Build number: 671. TimeStamp 20140611181230Z
commit db6818fa0c64ddc37368483dab945304492a824a Author: rodorad <rene.o.dorado@intel.com> Date: Tue Jun 10 10:50:46 2014 -0700 skip test in parent since they all get run on the children, exclude avro
%define TIMESTAMP %(echo 20140611181230Z)
%define TAR_FILE %(echo /home/rodorad/IdeaProjects/source_code/package/intelanalytics-graphbuilder-source.tar.gz)
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
/usr/lib/intelanalytics/graphbuilder/graphbuilder-3.jar
