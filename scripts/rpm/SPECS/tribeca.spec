Summary: Intel Tribeca Graph Analytics System

Version: 0.3.0

License: Apache

Group: Development

Name: tribeca

Requires: titan >= 0.4.0, python-ipython-notebook, python-matplotlib, scipy, python-pandas, sympy, python-nose

Prefix: /usr

Provides: 

Release: 1

Source: tribeca-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/tribecarpm

%description

The Intel Tribeca Graph Analytics System.

%prep

%setup -q

%build

%install

rm -fr $RPM_BUILD_ROOT

%clean

rm -rf $RPM_BUILD_ROOT

mvn clean

%files

