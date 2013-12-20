#!/bin/bash

pushd  `dirname $0`

python setup.py sdist --formats=zip,tar,gztar

popd
