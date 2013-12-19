Summary: Intel Graph Analytics System development libraries

Version: 0.5.0

License: Apache

Group: Development

Name: python-intelanalytics

Requires: python-intelanalytics-dependencies

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
ln -sf /usr/lib/IntelAnalytics/intel_analytics /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages

%postun

%files
%{_exec_prefix}/lib/IntelAnalytics

