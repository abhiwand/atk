Summary: Intel Analytics Toolkit - Python support libraries. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

Version: 0.8.0

License: Apache

Group: Development

Name: intelanalytics-python-deps-precompiled

Provides: intelanalytics-python-dependencies

Requires: intelanalytics, blas, bzip2, dos2unix, freetype, gtk2, libffi, libpng, ncurses, openssl, pygtk2, readline, sqlite, tk, tkinter, atlas, lapack, python-setuptools, yum-utils, zlib, boost, patch, perl-libwww-perl, zeromq

Prefix: /usr

Release: %{?BUILD_NUMBER}

Source: intelanalytics-python-deps-precompiled-%{version}.tar.gz

URL: https://www.intel.com

Buildroot: /tmp/intelanalytics_deps_rpm

%description
Install IPython and other dependencies for the Intel Analytics Toolkit. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

%define TIMESTAMP %(echo $TIMESTAMP)

%prep

%setup -q

%build


%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/



%clean

%post
ln -sf /usr/lib/IntelAnalytics/virtpy/bin/activate %{_bindir}/virtpy
tar xvf $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/template_overrides.tar.gz -C /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/IPython

%postun
rm %{_bindir}/virtpy

%files
%{_exec_prefix}/lib/IntelAnalytics

