
function pkg_check {
	if [[ ! -d /usr/lib/IntelAnalytics/virtpy/lib/python2.7/site-packages/$1 ]]; then
		echo ------------------------------------------------------
		echo Installing $1
        	pip install $1
	else
        	echo Module "$1" is installed
	fi
}

# The primary purpose of this script file is to allow the Sphinx-Build program
# to build in python 2.7 on a RedHat distribution of Linux. RedHat needs
# version 2.6 to run properly, so we create a virtual environment with
# python 2.7 in the PYTHONPATH when we create the documents.

if [[ -f /usr/lib/IntelAnalytics/virtpy/bin/activate ]]; then
    ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate
else
    ACTIVATE_FILE=/usr/local/virtpy/bin/activate
fi

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
    exit 1
fi

source $ACTIVATE_FILE

# If requests has not been installed, we need to do so.
# Comment out the make line lower down and run this script as superuser.
pkg_check requests
pkg_check cloud
pkg_check pandas