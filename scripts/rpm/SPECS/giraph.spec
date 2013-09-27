Summary: Giraph graph processing library

Version: 1.0.0

License: Apache

Group: Development

Name: giraph

Requires: hbase >= 0.94.7

Prefix: /usr

Release: 1

Source: giraph-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/giraphrpm

%description

The Giraph graph processing library. Packaged by Intel as part of the Tribeca project.

%prep

%setup -q

%build

mvn clean compile -DskipTests

%install

rm -fr ${buildroot}

mvn install -DskipTests

mkdir -p ${buildroot}/usr/lib
cp -R ${buildroot}/giraph-hbase/target/giraph-hbase-1.0.0* %{buildroot}/usr/lib/giraph

%clean

rm -rf ${buildroot}

mvn clean

%files

%{_exec_prefix}/lib/giraph


