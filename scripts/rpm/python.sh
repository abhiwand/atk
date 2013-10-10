cd ../../tribeca
pushd dist
virtualenv ENV
source ENV/bin/activate
popd
python setup.py install
pip install cairo
pip install numpy
pip install scipy
pip install matplotlib
pip install ipython
pip install pyzmq
pip install jinja2
pip install tornado
deactivate
virtualenv --relocatable dist/ENV
