Summary: Intel Graph Analytics System development libraries. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

Version: 0.8.0

License: Apache

Group: Intel Analytics

Name: intelanalytics-python

Requires: intelanalytics-python-dependencies

Prefix: /usr/lib/IntelAnalytics

Release: %{?BUILD_NUMBER}

Source: intelanalytics-python-%{version}.tar.gz

URL: <TODO>

Buildroot: /tmp/intelanalyticsrpm

%description
The Intel Big Data Analytics Tookit libraries for Python. Build number: %{BUILD_NUMBER}. Time %{TIMESTAMP}.

%define __os_install_post    \
    /usr/lib/rpm/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/brp-strip %{__strip}} \
    /usr/lib/rpm/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/brp-strip-comment-note %{__strip} %{__objdump} \
%{nil}


%define TIMESTAMP %(echo $TIMESTAMP)

%define IAUSER %(echo $IAUSER)

%prep

%setup -q

%build

%install

rm -fr %{buildroot}

mkdir -p %{buildroot}%{prefix}

cp -R * %{buildroot}%{prefix}

%clean

%post

installDir="/home/%{IAUSER}/"

ln -sf %{prefix}/intel_analytics %{prefix}/virtpy/lib/python2.7/site-packages
if [ ! -d ${installDir}.intelanalytics ]
then
    mkdir ${installDir}.intelanalytics
fi
if [ ! -d ${installDir}.intelanalytics/conf ]
then
    mkdir ${installDir}.intelanalytics/conf
fi

if [ ! -d ${installDir}.intelanalytics/logs ]
then
    mkdir ${installDir}.intelanalytics/logs
fi

if [ ! -d ${installDir}.intelanalytics/docs ]
then
    mkdir ${installDir}.intelanalytics/docs
fi

cp -R %{prefix}/docs/* /home/%{IAUSER}/.intelanalytics/docs

cp %{prefix}/conf/pig_log4j.properties /home/%{IAUSER}/.intelanalytics/conf/
if [ `ls %{prefix}/notebooks/*.ipynb | wc -l` -gt 0 ]
then
    mv %{prefix}/notebooks/*.ipynb  /home/%{IAUSER}/.intelanalytics/
fi
chown %{IAUSER}:%{IAUSER} -R /home/%{IAUSER}/.intelanalytics

cp  %{prefix}/conf/ipython.conf %{_sysconfdir}/init/

chmod +x %{prefix}/bin/set_ipython_password.sh
#echo "administrator" | %{prefix}/IntelAnalytics/bin/set_ipython_password.sh

%postun
rm %{_sysconfdir}/init/ipython.conf

%files
%{prefix}
#%{_exec_prefix}/lib/IntelAnalytics

