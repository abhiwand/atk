
. ./versions.sh

rpmbuild --define "_topdir ${PWD}" -bb SPECS/intelanalytics.spec
rpmbuild --define "_topdir ${PWD}" -bb SPECS/python-intelanalytics.spec
