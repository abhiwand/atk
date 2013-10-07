Summary: Intel Tribeca Graph Analytics System

Version: 0.5.0

License: Apache

Group: Development

Name: tribeca

Requires: java >= 1.7

Prefix: /usr

#Provides: 

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

#cd tribeca-$TRIBECA_VERSION

mkdir -p $RPM_BUILD_ROOT/usr/lib/tribeca
mkdir -p $RPM_BUILD_ROOT/usr/bin
mkdir -p $RPM_BUILD_ROOT/etc/tribeca

#cp -R tribeca-dist/tribeca-dist-all/target/tribeca-all-standalone $RPM_BUILD_ROOT/usr/lib/tribeca
#mv $RPM_BUILD_ROOT/usr/lib/tribeca/conf $RPM_BUILD_ROOT/etc/tribeca

#cp ../hbase-$HBASE_VERSION/target/hbase-$HBASE_VERSION-security.jar $RPM_BUILD_ROOT/usr/lib/tribeca/lib
#rm -f $RPM_BUILD_ROOT/usr/lib/tribeca/lib/$HBASE_VERSION.jar

#cp $RPM_BUILD_ROOT/usr/lib/tribeca/bin/gremlin.sh $RPM_BUILD_ROOT/usr/bin/tribeca-gremlin 
#echo "(cd /usr/lib/tribeca && bin/gremlin.sh)" > %{buildroot}/usr/bin/tribeca-gremlin

#chmod 755 %{buildroot}/usr/bin/tribeca-gremlin

#ln -sf %{_sysconfdir}/tribeca/conf %{buildroot}/usr/lib/tribeca/conf
#ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}/etc/tribeca/conf/hbase-env.sh
#ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}/etc/tribeca/conf/hbase-site.xml

%clean

rm -rf $RPM_BUILD_ROOT

%files

#%{_bindir}/tribeca-gremlin
%{_exec_prefix}/lib/tribeca
%{_sysconfdir}/tribeca


