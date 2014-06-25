Name: intelanalytics-graphbuilder
Summary: intelanalytics-graphbuilder-0.8 Build number: 901. TimeStamp 20140624195928Z
License: Confidential
Version: 0.8
Group: Intel Analytics
Requires: java >= 1.7, intelanalytics-spark-deps >= 0.8-901, jq
Release: 901
Source: intelanalytics-graphbuilder-0.8.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-graphbuilder-0.8 Build number: 901. TimeStamp 20140624195928Z
commit 06aee68eee188e14b34c781b828307450130f5d0 Merge: 5db0b28 2165283 Author: rodorad <rene.o.dorado@intel.com> Date: Tue Jun 24 12:04:01 2014 -0700 Merge remote-tracking branch 'origin/package' into sprint_14_package
%define TIMESTAMP %(echo 20140624195928Z)
%define TAR_FILE %(echo /home/rodorad/IdeaProjects/intellij/source_code/package/intelanalytics-graphbuilder-source.tar.gz)
%build
 cp %{TAR_FILE} %{_builddir}/files.tar.gz
%install
 rm -rf %{buildroot}
 mkdir -p %{buildroot}
 mv files.tar.gz %{buildroot}/files.tar.gz
 tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}
 rm %{buildroot}/files.tar.gz
%clean
%pre
%post

 #sim link to python sites packages
 if [ -f /usr/lib/intelanalytics/graphbuilder/ext/graphbuilder-3.jar ]; then
   rm /usr/lib/intelanalytics/graphbuilder/ext/graphbuilder-3.jar
 fi

 ln -s /usr/lib/intelanalytics/graphbuilder/graphbuilder-3.jar  /usr/lib/intelanalytics/graphbuilder/ext/graphbuilder-3.jar

%preun
%postun



%files

/usr/lib/intelanalytics/graphbuilder/bin
/usr/lib/intelanalytics/graphbuilder/conf
/usr/lib/intelanalytics/graphbuilder/ext

/usr/lib/intelanalytics/graphbuilder/set-cm-spark-classpath.sh
/usr/lib/intelanalytics/graphbuilder/graphbuilder-3.jar
