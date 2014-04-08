Name: intelanalytics
Provides: intelanalytics
Summary: intelanalytics-0.8.0 Build number: 123. TimeStamp 20140408000656Z
License: Apache
Version: 0.8.0
Group: Intel Analytics
Release: 123
Source: intelanalytics-0.8.0.tar.gz
%description
intelanalytics-0.8.0 Build number: 123. TimeStamp 20140408000656Z
%define TIMESTAMP %(echo 20140408000656Z)
%define TAR_FILE %(echo /home/ubuntu/source.tar.gz)
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
/usr/lib/intelanalytics/4rf
/usr/lib/intelanalytics/test.txt
