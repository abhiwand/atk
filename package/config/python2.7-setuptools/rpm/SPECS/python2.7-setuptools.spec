Name: python2.7-setuptools
Provides: python-abi = %{libvers}
Provides: python(abi) = %{libvers}
Summary: setuptools %{?setuptoolsVersion} rpm built against python %{?pythonVersion}
License: https://pypi.python.org/pypi/setuptools
Release: 1
Version: %{?setuptoolsVersion}
BuildRequires: gcc make expat-devel db4-devel gdbm-devel sqlite-devel readline-devel zlib-devel bzip2-devel openssl-devel python2.7
Requires: python2.7, gcc, gcc-c++
AutoReq: no
Source: setuptools-3.6.tar.gz
Prefix: /usr
URL: https://github.com/pypa/pip
%description
setuptools %{setuptoolsVersion} rpm built against python %{pythonVersion}

%define pythonVersion 2.7.5
%define pipVersion 3.6

%prep

 tar -xvf %{_sourcedir}/setuptools-%{version}.tar.gz
 mv setuptools-%{version}/* .
 rm -rf setuptools-%{version}

%build

 python2.7 setup.py build

%install

 rm -rf %{buildroot}

 mkdir -p %{buildroot}

 export PYTHONPATH=%{buildroot}%{prefix}/lib/python2.7/site-packages
 
 mkdir -p %{buildroot}%{prefix}/lib/python2.7/site-packages

 python2.7 setup.py install  --prefix %{buildroot}%{prefix}

 mv %{buildroot}/%{prefix}/bin/easy_install  %{buildroot}/%{prefix}/bin/easy_install2.7
 
 rm %{buildroot}%{prefix}/lib/python2.7/site-packages/site.py
 rm %{buildroot}%{prefix}/lib/python2.7/site-packages/site.pyc
 rm %{buildroot}%{prefix}/lib/python2.7/site-packages/easy-install.pth
%clean
%post
 
%postun
 
%files
%{prefix}/*
