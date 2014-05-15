Name: python2.7-pip
Provides: python-abi = %{libvers}
Provides: python(abi) = %{libvers}
Summary: pip %{?pipVersion} rpm built against python %{?pythonVersion}
License: https://github.com/pypa/pip/blob/develop/LICENSE.txt
Release: 1
Version: %{?pipVersion}
BuildRequires: gcc make expat-devel db4-devel gdbm-devel sqlite-devel readline-devel zlib-devel bzip2-devel openssl-devel
Requires: python2.7, gcc, gcc-c++
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

%clean
%post
 
%postun
 
%files
%{prefix}/*
