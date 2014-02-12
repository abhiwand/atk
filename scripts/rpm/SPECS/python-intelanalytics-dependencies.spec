Summary: Intel Graph Analytics System development libraries. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

Version: 0.8.0

License: Apache

Group: Development

Name: python-intelanalytics-dependencies

Requires: java >= 1.7, blas,  bzip2-devel,  dos2unix,  freetype-devel,  gcc,  gtk2-devel,  libffi-devel,  libpng-devel,  ncurses-devel,  openssl-devel,  pygtk2-devel,  python-devel,  readline-devel,  sqlite-devel,  tk-devel,  tkinter, atlas, atlas-devel, blas-devel, freetype, freetype-devel, gcc-c++, lapack, lapack-devel, libpng-devel, python-devel, python-setuptools, yum-utils, zlib-devel, boost-devel, intelanalytics

Prefix: /usr

Release: %{?BUILD_NUMBER}

Source: python-intelanalytics-dependencies-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanalyticsrpm

%description
Install IPython and Intel Graph System Python dependencies. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

%define TIMESTAMP %(echo $TIMESTAMP)

%prep

%setup -q

%build

%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/ipython

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/

%clean

%post
$RPM_BUILD_ROOT/usr/lib/IntelAnalytics/install_pyenv.sh #install virtual python
tar xvf $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/template_overrides.tar.gz -C /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/IPython
ln -sf /usr/lib/IntelAnalytics/virtpy/bin/activate %{_bindir}/virtpy

%postun
if [ "$1" = "0" ]; then # $1 is set to 0 for rpm uninstall and 1 for update
  rm -rf /usr/lib/IntelAnalytics/virtpy #remove vitual python 
  rm %{_bindir}/virtpy
fi

%files
%{_exec_prefix}/lib/IntelAnalytics

