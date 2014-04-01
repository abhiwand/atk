Summary: Intel Graph Analytics Aws scripts. Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

Version: %{?VERSION}

License: Confidential

Group: Development

Name: intelanalytics-aws

Release: %{?BUILD_NUMBER}

Source: intelanalytics-aws-%{version}.tar.gz

Buildroot: /tmp/intelanayltics-aws-rpm

%description
The Intel Graph Analytics aws scripts. Build number: Build number: %{?BUILD_NUMBER}. Time %{?TIMESTAMP}.

%define TIMESTAMP %(echo $TIMESTAMP)

%define VERSION %(echo $TRIBECA_VERSION)

%prep

%setup -q

%build

%install

rm -rf $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/home/ec2-user/intel/IntelAnalytics/%{?VERSION}/

cp -R * $RPM_BUILD_ROOT/home/ec2-user/intel/IntelAnalytics/%{?VERSION}/

chmod 755 $RPM_BUILD_ROOT/home/ec2-user/intel/IntelAnalytics/%{?VERSION}/deploy/scripts/*

%clean

rm -rf $RPM_BUILD_ROOT

%files
/home/ec2-user/intel/IntelAnalytics/%{?VERSION}/*

