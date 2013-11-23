Summary: Intel Graph Analytics System development libraries

Version: 0.5.0

License: Apache

Group: Development

Name: IntelAnalytics-devel

Requires: java >= 1.7, blas,  bzip2-devel,  dog2unix,  freetype-devel,  gcc,  gtk2-devel,  libffi-devel,  libpng-devel,  ncurses-devel,  openssl-devel,  pygtk2-devel,  python-devel,  readline-devel,  sqlite-devel,  tk-devel,  tkinter, atlas, atlas-devel, blas-devel, freetype, freetype-devel, gcc-c++, lapack, lapack-devel, libpng-devel, python-devel, python-setuptools, yum-utils, zlib-devel

Prefix: /usr

Release: 1

Source: IntelAnalytics-devel-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/IntelAnalyticsrpm

%description

The Intel Graph System Python libraries

%prep

%setup

%build

%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/

%clean

%post
$RPM_BUILD_ROOT/usr/lib/IntelAnalytics/install_pyenv.sh #install virtual python
ln -sf /usr/lib/IntelAnalytics/virtpy/bin/activate %{_bindir}/virtpy

#untar source to python
tar xvf /usr/lib/IntelAnalytics/intel_analytics.tar.gz -C /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/

%postun
rm -rf /usr/lib/IntelAnalytics/virtpy #remove vitual python 
rm %{_bindir}/virtpy

%files
%{_exec_prefix}/lib/IntelAnalytics

