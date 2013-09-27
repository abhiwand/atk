Summary: Titan distributed graph database

Version: 0.4.0

License: Apache

Group: Databases

Name: titan

Requires: hbase >= 0.94.7

Prefix: /usr

Provides: gremlin

Release: 1

Source: titan-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/titanrpm

%description

The Titan distributed graph database. Packaged by Intel as part of the Tribeca project.

%prep

%setup -q

%build

mvn clean compile -DskipTests

%install

rm -fr $RPM_BUILD_ROOT

mvn install -DskipTests

mkdir -p $RPM_BUILD_ROOT/usr/lib
mkdir -p $RPM_BUILD_ROOT/usr/bin
mkdir -p $RPM_BUILD_ROOT/etc/titan
cp -R titan-dist/titan-dist-all/target/titan-all-standalone $RPM_BUILD_ROOT/usr/lib/titan
mv $RPM_BUILD_ROOT/usr/lib/titan/config $RPM_BUILD_ROOT/etc/titan
#cp $RPM_BUILD_ROOT/usr/lib/titan/bin/gremlin.sh $RPM_BUILD_ROOT/usr/bin/titan-gremlin 
echo "(cd /usr/lib/titan && bin/gremlin.sh)" > %{buildroot}/usr/bin/titan-gremlin

chmod 755 %{buildroot}/usr/bin/titan-gremlin

ln -sf %{_sysconfdir}/titan/config %{buildroot}/usr/lib/titan/config
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}/etc/titan/config/hbase-env.sh
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}/etc/titan/config/hbase-site.xml
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-policy.xml %{buildroot}/etc/titan/config/hbase-policy.xml
%clean

rm -rf $RPM_BUILD_ROOT

mvn clean

%files

%{_bindir}/titan-gremlin
%{_exec_prefix}/lib/titan
%{_sysconfdir}/titan/config


