# Local macros
%define __jar_repack    %{nil}
%define build_timestamp %(date +"%Y%m%d%H%M%S")
%define _prefix         /usr/lib
%define titan_name      titan-server
%define titan_user      %{?TITAN_USER}%{!?TITAN_USER:hadoop}
%define titan_group     %{?TITAN_GROUP}%{!?TITAN_GROUP:hadoop}
%define titan_version   %{?TITAN_VERSION}%{!?TITAN_VERSION:0.4.2}
%define titan_build     %{?BUILD_NUMBER}%{!?BUILD_NUMBER:0000}
%define tribeca_version %{?TRIBECA_VERSION}%{!?TRIBECA_VERSION:0.8.0}
%define titan_sourcepkg %{titan_name}-%{titan_version}_%{tribeca_version}.tar.gz
# replace w/ gaomaven as permanant location
%define titan_sourceurl "http://zydevhf.hf.intel.com/public"
%define titan_instdir   %{_prefix}/%{titan_name}
%define titan_logsdir   %{_localstatedir}/log/titan
%define titan_pidsdir   %{_localstatedir}/run/titan
%define titan_upstart   %{_sysconfdir}/init/titan-server.conf
%define titan_srvconf   %{?TITAN_SRVCONF}%{!?TITAN_SRVCONF:%{_specdir}/titan-server.conf}
%define titan_rexconf   %{titan_instdir}/conf/rexstitan-hbase-es.xml

# RPM package info
Summary:                Titan Rexster Server for Intel Big Data Analytics Toolkit.
Vendor:                 Intel Corp.
#CopyRight:             Intel Corp. ??
Name:                   %{titan_name}
Version:                %{titan_version}_%{tribeca_version}
Release:                %{titan_build}
License:                Apache
Group:                  Applications/System
#URL:                   http://intelanalytics.intel.com ??
Source0:                %{titan_sourcepkg}
Requires:               java >= 1.7

%description
Titan Graph Database Rexster Server for Intel Big Data Analytics Toolkit.

%build
rm -f %{SOURCE0}
wget -O %{SOURCE0} %{titan_sourceurl}/%{titan_sourcepkg}

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}%{titan_instdir}/
mkdir -p %{buildroot}%{titan_logsdir}/
mkdir -p %{buildroot}%{titan_pidsdir}/
mkdir -p %{buildroot}%{_sysconfdir}/init/
tar zxf %{SOURCE0} -C %{buildroot}%{titan_instdir}
cp --force %{titan_srvconf} %{buildroot}%{titan_upstart}

%clean
rm -rf %{buildroot}

%pre
# stop titan if it is currently running
echo Stopping previously installed Titan Server, if any...
initctl stop %{titan_name} > /dev/null 2>&1
exit 0

%post
chown %{titan_user}:%{titan_group} -R %{titan_instdir}
mkdir -p %{titan_logsdir} > /dev/null 2>&1
rm -rf %{titan_instdir}/logs > /dev/null 2>&1
ln -s %{titan_logsdir} %{titan_instdir}/logs
chown %{titan_user}:%{titan_group} -R %{titan_logsdir}
chown %{titan_user}:%{titan_group} -R %{titan_pidsdir}
echo Starting newly installed Titan Server...
initctl start %{titan_name} > /dev/null 2>&1
exit 0

%preun
if [ $1 = 0 ]; then
    echo Stopping the running Titan Server...
    initctl stop %{titan_name} > /dev/null 2>&1
    exit 0
fi

%postun
# further cleanups

%files
%{titan_instdir}
%config %{titan_rexconf}
%config %{titan_upstart}
%attr(644,-, -) %{titan_rexconf}
%attr(644,root,root) %{titan_upstart}
%dir %attr(755,-,-) %{titan_logsdir}
%dir %attr(755,-,-) %{titan_pidsdir}

%changelog
* Thu Mar 14 2014 - yi.zou (at) intel.com
- Added Titan Server Package
