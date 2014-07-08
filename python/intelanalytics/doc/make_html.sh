# The primary purpose of this script file is to allow the Sphinx-Build program
# to build in python 2.7 on a RedHat distribution of Linux. RedHat needs
# version 2.6 to run properly, so we create a virtual environment with
# python 2.7 in the PYTHONPATH when we create the documents.
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

echo $SCRIPT


OS=$(cat /etc/*-release | grep ubuntu)

function no_virtpy_exit_os_check()
{
	if [ "$OS" == "" ]; then
		exit 1
	fi
	echo "running on ubuntu we don't need virtpy because ubuntu is awesome"
}

if [[ -f /usr/lib/IntelAnalytics/virtpy/bin/activate ]]; then
    ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate
else
    ACTIVATE_FILE=/usr/local/virtpy/bin/activate
fi

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
	no_virtpy_exit_os_check
fi

if [ "$OS" == "" ]; then
	source $ACTIVATE_FILE
fi

pushd $SCRIPTPATH

# if the individual running this script is a techwriter and has started the script with 
# the key word 'packages', check that the packages have been installed.
if [ -f techwriters ]
then
    HUMAN=$( pwd | grep -c --file=techwriters)
    if [ $HUMAN -gt 0 ]
    then
        COMPILE=$( echo $1 | grep -ic "packages" )
        if [ $COMPILE -gt 0 ]; then
            clear
            # We need to check the installed packages and insure they are complete.
            # The packages need superuser rights to install properly.
            sudo ./install_packages
        fi
    fi
fi

# We assume we are running from the doc directory. Ignore all toctree warnings.
# Check for weird stuff first
if
    weird=$( echo $1 | grep -ic -e "text" -e "doctest" -e "latexpdf" -e "latex")
then
    # Delete previously automatically built files
    built_dir="build/"$1
    if [[ -d $built_dir ]]; then
        echo Deleting previous build.
        rm -R $built_dir
    fi

    make -B $1 2>&1 | grep -v -f toctreeWarnings
else
    # Delete previously automatically built files
    if [[ -d build/html ]]; then
        echo Deleting previous build.
        rm -R build/html
    fi

    make -B html 2>&1 | grep -v -f toctreeWarnings
fi
# Make a zip file of that which was just built
if [[ -d build ]]; then
    zip -rq intel_analytics_docs.zip build
fi
popd
