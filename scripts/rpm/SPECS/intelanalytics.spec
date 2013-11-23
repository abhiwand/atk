Summary: Intel Graph Analytics System

Version: 0.5.0

License: Apache

Group: Development

Name: intelanalytics

Requires: java >= 1.7

Prefix: /usr

Release: 2

Source: intelanalytics-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanaylticsrpm

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

ln -sf %{_sysconfdir}/IntelAnalytics %{buildroot}/usr/lib/IntelAnalytics/conf
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}/etc/IntelAnalytics/hbase-env.sh
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}/etc/IntelAnalytics/hbase-site.xml
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-env.sh %{buildroot}/etc/IntelAnalytics/hadoop-env.sh
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-site.xml %{buildroot}/etc/IntelAnalytics/hadoop-site.xml


%clean

rm -rf $RPM_BUILD_ROOT

%files

%{_exec_prefix}/lib/IntelAnalytics
%{_sysconfdir}/IntelAnalytics


