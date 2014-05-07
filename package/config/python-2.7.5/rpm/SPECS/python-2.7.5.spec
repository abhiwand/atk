Name: python-2.7.5
Provides: python-abi = %{libvers}
Provides: python(abi) = %{libvers}
Summary: python-2.7.5- Build number: 32. TimeStamp 20140506054731Z
License: Apache
Version: 2.7.5
Group: Intel Analytics
BuildRequires: gcc make expat-devel db4-devel gdbm-devel sqlite-devel readline-devel zlib-devel bzip2-devel openssl-devel
Requires: openssl, glibc
Release: 35
AutoReq: no
Source: Python-2.7.5.tar.bz2
URL: graphtrial.intel.com
%description
bleh
#python-2.7.5- Build number: 32. TimeStamp 20140506054731Z
#%define TIMESTAMP %(echo 20140506054731Z)
#%define TAR_FILE %(echo 0.8.0)
%build
 echo "BUILD"
 cp %{_sourcedir}/Python-2.7.5.tar.bz2 %{_builddir}/Python-2.7.5.tar.bz2
 tar -xvf %{_builddir}/Python-2.7.5.tar.bz2 -C %{_builddir}
 cd Python-2.7.5/
 ./configure --prefix=%{buildroot}/usr/local/python2.7/
 #make -j4
 make %{_smp_mflags}

%install
 rm -rf %{buildroot}

 mkdir -p %{buildroot}

 #mv Python-2.7.5.tar.bz2 %{buildroot}/Python-2.7.5.tar.bz2
 
 #tar -xvf %{buildroot}/Python-2.7.5.tar.bz2 -C %{buildroot}
 
 #rm  %{buildroot}/Python-2.7.5.tar.bz2
 
 #cd %{buildroot}/Python-2.7.5/

 #./configure --prefix=%{buildroot}/usr/local/python2.7/

 #make -j4
 cd Python-2.7.5
 pwd
 make altinstall 
 
 rm -rf %{buildroot}/Python-2.7.5/
 #mkdir -p %{buildroot}
 #mv files.tar.gz %{buildroot}/files.tar.gz
 #tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}
 #rm %{buildroot}/files.tar.gz
%clean
%post
%postun
%files
/usr/local/python2.7/
