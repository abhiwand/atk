Summary: Faunus graph analytics library

Version: 0.4.0

License: Apache

Group: Development

Name: faunus

Requires: hbase >= 0.94.7

Prefix: /usr

Provides: gremlin

Release: 1

Source: faunus-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/faunusrpm

%description

The Faunus distributed graph database. Packaged by Intel as part of the Tribeca project.

%prep

%setup -q

%build

mvn clean compile -DskipTests

%install

rm -fr $RPM_BUILD_ROOT

mvn install -DskipTests

mkdir -p $RPM_BUILD_ROOT/usr/lib/faunus
mkdir -p $RPM_BUILD_ROOT/usr/bin
cp target/faunus-0.4.0*.jar $RPM_BUILD_ROOT/usr/lib/faunus
echo "(cd /usr/lib/faunus && bin/gremlin.sh)" > $RPM_BUILD_ROOT/usr/bin/faunus-gremlin

chmod 755 $RPM_BUILD_ROOT/usr/bin/faunus-gremlin

%clean

rm -rf $RPM_BUILD_ROOT

mvn clean

%files

%{_bindir}/faunus-gremlin
%{_exec_prefix}/lib/faunus


