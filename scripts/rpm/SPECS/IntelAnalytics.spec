Summary: Intel Graph Analytics System

Version: 0.5.0

License: Apache

Group: Development

Name: IntelAnalytics

Requires: java >= 1.7

Prefix: /usr

Provides: IntelAnalytics-gremlin

Release: 2

Source: IntelAnalytics-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/IntelAnaylticsrpm

%description

The Intel Graph Analytics System.

%prep

%setup -q

%build

%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics
mkdir -p $RPM_BUILD_ROOT/usr/bin
mkdir -p $RPM_BUILD_ROOT/etc/IntelAnalytics

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

cp $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/bin/IntelAnalytics-gremlin $RPM_BUILD_ROOT/usr/bin/IntelAnalytics-gremlin 
echo "(cd /usr/lib/IntelAnalytics && bin/IntelAnalytics-gremlin)" > %{buildroot}/usr/bin/IntelAnalytics-gremlin

chmod 755 %{buildroot}/usr/bin/IntelAnalytics-gremlin

ln -sf %{_sysconfdir}/IntelAnalytics %{buildroot}/usr/lib/IntelAnalytics/conf
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}/etc/IntelAnalytics/hbase-env.sh
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}/etc/IntelAnalytics/hbase-site.xml
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-env.sh %{buildroot}/etc/IntelAnalytics/hadoop-env.sh
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-site.xml %{buildroot}/etc/IntelAnalytics/hadoop-site.xml

%clean

rm -rf $RPM_BUILD_ROOT

%post
$RPM_BUILD_ROOT/usr/lib/IntelAnalytics/install_pyenv.sh #install virtual python
ln -sf /usr/lib/IntelAnalytics/virtpy/bin/activate %{_bindir}/virtpy

%postun
rm -rf /usr/lib/IntelAnalytics/virtpy #remove vitual python 
rm %{_bindir}/virtpy

%files

%{_bindir}/IntelAnalytics-gremlin
%{_exec_prefix}/lib/IntelAnalytics
%{_sysconfdir}/IntelAnalytics


