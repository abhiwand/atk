Summary: Faunus graph analytics library

Version: 0.3.2

License: Apache

Group: Development

Name: faunus

Requires: hbase >= 0.94.7

Prefix: /usr

Provides: gremlin

Release: 1

URL: <TODO>

Buildroot: /tmp/faunusrpm

%description

The Faunus distributed graph database. Packaged by Intel as part of the Tribeca project.

%prep

tar xf ../SOURCES/faunus-$FAUNUS_VERSION.tar.gz

%build

cd faunus-$FAUNUS_VERSION

mvn clean compile -DskipTests

%install

rm -fr $RPM_BUILD_ROOT

cd faunus-$FAUNUS_VERSION

mvn install -DskipTests

mkdir -p $RPM_BUILD_ROOT/usr/lib/titan/ext/faunus

cp target/faunus-*.jar $RPM_BUILD_ROOT/usr/lib/titan/ext/faunus

%clean

rm -rf $RPM_BUILD_ROOT

cd faunus-$FAUNUS_VERSION

mvn clean

%files

%{_exec_prefix}/lib/titan/ext/faunus


