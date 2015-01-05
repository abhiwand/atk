Name: intelanalytics
Summary: intelanalytics- Build number: 900. TimeStamp Mon Nov 17 14:26:08 PST 2014
License: Apache
Version: 0.9.0
Group: Intel Analytics
Release: 900
Source: intelanalytics-0.9.0.tar.gz
URL: intel.com
%description
intelanalytics- Build number: 900. TimeStamp Mon Nov 17 14:26:08 PST 2014
%define TIMESTAMP %(echo Mon Nov 17 14:26:08 PST 2014)
%define TAR_FILE %(echo 0.9.0)
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
