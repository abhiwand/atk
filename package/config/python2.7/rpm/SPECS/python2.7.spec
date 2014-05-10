Name: python2.7
Provides: python-abi = %{libvers}
Provides: python(abi) = %{libvers}
Summary: compiled python %{pythonVersion}
License: PSF
Version: %{pythonVersion}
BuildRequires: gcc make expat-devel db4-devel gdbm-devel sqlite-devel readline-devel zlib-devel bzip2-devel openssl-devel
Requires: openssl, glibc, gcc, gcc-c++
Release: 1
AutoReq: no
Source: Python-%{pythonVersion}.%{tarSuffix}
Prefix: /usr
URL: https://www.python.org/download/releases/%{pythonVersion}
%description
compiled python %{pythonVersion}

%define libVersion 2.7
%define pythonVersion 2.7.5
%define tarSuffix tgz
%define pythonSource Python-%{pythonVersion}.%{tarSuffix}


%prep

 tar -xvf %{_sourcedir}/%{pythonSource}

 mv Python-%{pythonVersion}/* .
 
 rm -rf Python-%{pythonVersion}

%build

 ./configure --prefix=%{buildroot}%{prefix}
 
 make %{_smp_mflags}

%install

 make altinstall 
 
 rm -rf %{buildroot}/Python-2.7.5/

 #  REPLACE PATH IN PYDOC
      cd %{buildroot}%{prefix}/bin
      mv pydoc pydoc.old
      sed 's|#!.*|#!%{prefix}/bin/python'%{libVersion}'|' \
            pydoc.old >pydoc
      chmod 755 pydoc
      rm -f pydoc.old
      sed -i -e 's|#!.*|#!%{prefix}/bin/python'%{libVersion}'|' python%{libVersion}-config
 
    cd %{buildroot}%{prefix}/bin;
      for file in 2to3  pydoc  idle smtpd.py; do [ -f "$file" ] && mv "$file" %{name}"$file"; done;


%clean
%post
 
%postun
 
%files
%{prefix}/*
