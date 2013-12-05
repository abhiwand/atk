Summary: Intel Graph Analytics System development libraries

Version: 0.5.0

License: Apache

Group: Development

Name: python-intelanalytics

Requires: java >= 1.7, blas,  bzip2-devel,  dos2unix,  freetype-devel,  gcc,  gtk2-devel,  libffi-devel,  libpng-devel,  ncurses-devel,  openssl-devel,  pygtk2-devel,  python-devel,  readline-devel,  sqlite-devel,  tk-devel,  tkinter, atlas, atlas-devel, blas-devel, freetype, freetype-devel, gcc-c++, lapack, lapack-devel, libpng-devel, python-devel, python-setuptools, yum-utils, zlib-devel, boost-devel, python-intelanalytics-dependencies

Prefix: /usr

Release: %{?BUILD_NUMBER}

Source: python-intelanalytics-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanalyticsrpm

%description

The Intel Graph System Python libraries

%prep

%setup -q

%build

%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/

%clean

%post

%postun

%files
%{_exec_prefix}/lib/IntelAnalytics

