Name: intelanalytics
Provides: intelanalytics
Summary: intelanalytics-0.8.0 Build number: 1111. TimeStamp 20140410202311Z
License: Apache
Version: 0.8.0
Group: Intel Analytics
Release: 1111
Source: intelanalytics-0.8.0.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-0.8.0 Build number: 1111. TimeStamp 20140410202311Z
%define TIMESTAMP %(echo 20140410202311Z)
%define TAR_FILE %(echo /home/rodorad/IdeaProjects/source_code/package/source.tar.gz)
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
/usr/lib/intelanalytics/intel-analytics_2.10-0.8.jar
