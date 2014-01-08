Summary: Intel Graph Analytics System

Version: 0.5.0

License: Apache

Group: Development

Name: intelanalytics

Requires: java >= 1.7

Prefix: /usr

Release: %{BUILD_NUMBER}

Source: intelanalytics-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanaylticsrpm

%description

The Intel Graph Analytics System.

%define __os_install_post    \
    /usr/lib/rpm/redhat/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/redhat/brp-strip %{__strip}} \
    /usr/lib/rpm/redhat/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/redhat/brp-strip-comment-note %{__strip} %{__objdump} \
    /usr/lib/rpm/brp-python-bytecompile \
    /usr/lib/rpm/redhat/brp-python-hardlink \
%{nil}

%prep

%setup -q

%build

%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics
mkdir -p $RPM_BUILD_ROOT/usr/bin
mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/conf
mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/ipython
mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/target

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

#ln -sf %{buildroot}/usr/lib/IntelAnalytics/conf %{_sysconfdir}/IntelAnalytics
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-env.sh %{buildroot}/usr/lib/IntelAnalytics/conf/hbase-env.sh
ln -sf %{_sysconfdir}/hbase/conf.dist/hbase-site.xml %{buildroot}/usr/lib/IntelAnalytics/conf/hbase-site.xml
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-env.sh %{buildroot}/usr/lib/IntelAnalytics/conf/hadoop-env.sh
ln -sf %{_sysconfdir}/hadoop/conf/hadoop-site.xml %{buildroot}/usr/lib/IntelAnalytics/conf/hadoop-site.xml

%post
SITE_PACKAGES=/usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/
mkdir -p $SITE_PACKAGES

#untar source to python
tar xvf /usr/lib/IntelAnalytics/intel_analytics.tar.gz -C /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/

ln -sf $SITE_PACKAGES/intel_analytics/intel_analytics.properties /usr/lib/IntelAnalytics/conf/intel_analytics.properties

%postun
rm -rf $SITE_PACKAGES/intel_analytics
%clean

rm -rf $RPM_BUILD_ROOT

%files

%{_exec_prefix}/lib/IntelAnalytics
#%{_sysconfdir}/IntelAnalytics


