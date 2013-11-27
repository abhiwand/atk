
. ./versions.sh

rpmbuild --clean --define "_topdir ${PWD}" -bb SPECS/intelanalytics.spec
rpmbuild --clean --define "_topdir ${PWD}" -bb SPECS/python-intelanalytics.spec
