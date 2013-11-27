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
mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/conf

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

#ln -sf %{buildroot}/usr/lib/IntelAnalytics/conf %{_sysconfdir}/IntelAnalytics
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}/usr/lib/IntelAnalytics/conf/hbase-env.sh
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}/usr/lib/IntelAnalytics/conf/hbase-site.xml
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-env.sh %{buildroot}/usr/lib/IntelAnalytics/conf/hadoop-env.sh
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-site.xml %{buildroot}/usr/lib/IntelAnalytics/conf/hadoop-site.xml


%clean

rm -rf $RPM_BUILD_ROOT

%files

%{_exec_prefix}/lib/IntelAnalytics
#%{_sysconfdir}/IntelAnalytics


