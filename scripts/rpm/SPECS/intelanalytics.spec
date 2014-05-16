Summary: Intel Graph Analytics System. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

Version: 0.8.0

License: Apache

Group: Intel Analytics

Name: intelanalytics

Requires: java >= 1.7

Prefix: /usr/lib/IntelAnalytics

Release: %{?BUILD_NUMBER}

Source: intelanalytics-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanaylticsrpm

%description
The Intel Graph Analytics System. Build number: Build number: %{BUILD_NUMBER}. Time %{TIMESTAMP}.

%define __os_install_post    \
    /usr/lib/rpm/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/brp-strip %{__strip}} \
    /usr/lib/rpm/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/brp-strip-comment-note %{__strip} %{__objdump} \
%{nil}

%define TIMESTAMP %(echo $TIMESTAMP)

%prep

%setup -q

%build

%install

rm -fr %{buildroot}

mkdir -p %{buildroot}%{prefix}
#mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}%{prefix}/conf
mkdir -p %{buildroot}%{prefix}/target

cp -R * %{buildroot}%{prefix}

ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}%{prefix}/conf/hbase-env.sh
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}%{prefix}/conf/hbase-site.xml
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-env.sh %{buildroot}%{prefix}/conf/hadoop-env.sh
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-site.xml %{buildroot}%{prefix}/conf/hadoop-site.xml


%clean

rm -rf $RPM_BUILD_ROOT

%files
%{prefix}
#%{_exec_prefix}/lib/IntelAnalytics
#%{_sysconfdir}/IntelAnalytics


