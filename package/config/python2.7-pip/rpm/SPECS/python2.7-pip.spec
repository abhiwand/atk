Name: python2.7-pip
Provides: python-abi = %{libvers}
Provides: python(abi) = %{libvers}
Summary: pip %{?pipVersion} rpm built against python %{?pythonVersion}
License: https://github.com/pypa/pip/blob/develop/LICENSE.txt
Release: 1
Version: %{?pipVersion}
BuildRequires: gcc >= 4.4.7, make, expat-devel, db4-devel, gdbm-devel, sqlite-devel, readline-devel, zlib-devel, bzip2-devel, openssl-devel
Requires: python2.7, python2.7-setuptools, gcc >= 4.4.7, gcc-c++
AutoReq: no
Source: pip-1.5.5.tar.gz
Prefix: /usr
URL: https://github.com/pypa/pip
%description
pip %{pipVersion} rpm built against python %{pythonVersion}

%define pythonVersion 2.7.5
%define pipVersion 1.5.5

%prep

 tar -xvf %{_sourcedir}/pip-%{version}.tar.gz
 mv pip-%{version}/* .
 rm -rf pip-%{version}

%build

 python2.7 setup.py build 
 
%install

 rm -rf %{buildroot}

 mkdir -p %{buildroot}

 export PYTHONPATH=%{buildroot}%{prefix}/lib/python2.7/site-packages

 mkdir -p %{buildroot}%{prefix}/lib/python2.7/site-packages

 python2.7 setup.py install  --prefix %{buildroot}%{prefix}
 
 rm %{buildroot}%{prefix}/bin/pip
 rm %{buildroot}%{prefix}/bin/pip2 
 rm %{buildroot}%{prefix}/lib/python2.7/site-packages/site.py
 rm %{buildroot}%{prefix}/lib/python2.7/site-packages/site.pyc
 rm %{buildroot}%{prefix}/lib/python2.7/site-packages/easy-install.pth

%clean
%post
 
%postun
 
%files
%{prefix}/*
