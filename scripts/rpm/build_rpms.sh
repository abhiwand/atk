
. ./versions.sh

rpmbuild --define "_topdir ${PWD}" -bb SPECS/intelanalytics.spec

