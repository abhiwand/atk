Summary: Intel Graph Analytics System development libraries. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

Version: 0.8.0

License: Apache

Group: Development

Name: intelanalytics-python

Requires: intelanalytics-python-dependencies

Prefix: /usr

Release: %{?BUILD_NUMBER}

Source: intelanalytics-python-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanalyticsrpm

%description
The Intel Big Data Analytics Tookit libraries for Python. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

%define __os_install_post    \
    /usr/lib/rpm/redhat/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/redhat/brp-strip %{__strip}} \
    /usr/lib/rpm/redhat/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/redhat/brp-strip-comment-note %{__strip} %{__objdump} \
%{nil}

%define TIMESTAMP %(echo $TIMESTAMP)

%prep

%setup -q

%build

%install

rm -fr $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/usr/lib/IntelAnalytics

cp -R * $RPM_BUILD_ROOT/usr/lib/IntelAnalytics/

%clean

%post
ln -sf /usr/lib/IntelAnalytics/intel_analytics /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages
if [ ! -d /home/hadoop/.intelanalytics ]
then
    mkdir /home/hadoop/.intelanalytics
fi
if [ ! -d /home/hadoop/.intelanalytics/conf ]
then
    mkdir /home/hadoop/.intelanalytics/conf
fi

if [ ! -d /home/hadoop/.intelanalytics/logs ]
then
    mkdir /home/hadoop/.intelanalytics/logs
fi

cp /usr/lib/IntelAnalytics/conf/pig_log4j.properties /home/hadoop/.intelanalytics/conf/
if [ `ls /usr/lib/IntelAnalytics/notebooks/*.ipynb | wc -l` -gt 0 ]
then
    mv /usr/lib/IntelAnalytics/notebooks/*.ipynb  /home/hadoop/.intelanalytics/
fi
chown hadoop:hadoop -R /home/hadoop/.intelanalytics

%postun

%files
%{_exec_prefix}/lib/IntelAnalytics

