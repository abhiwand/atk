

rpmbuild --define "_topdir ${PWD}" -bb SPECS/titan.spec
rpmbuild --define "_topdir ${PWD}" -bb SPECS/faunus.spec
rpmbuild --define "_topdir ${PWD}" -bb SPECS/giraph.spec


