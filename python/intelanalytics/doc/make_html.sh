# The primary purpose of this script file is to allow the Sphinx-Build program
# to build in python 2.7 on a RedHat distribution of Linux. RedHat needs
# version 2.6 to run properly, so we create a virtual environment with
# python 2.7 in the PYTHONPATH when we create the documents.

# Running on Ubuntu, we don't need virtpy because Ubuntu is awesome!
UBUNTU_OS=$(cat /etc/*-release | grep -i ubuntu)

# Look for help
echo "$1 $2 $3 $4 $5 $6 $7 $8 $9" | grep -i -e "-h" > /dev/null
if [ $? == 0 ]; then
    echo
    echo $0
    echo
    echo "-h, --help: Print this and exit"
    if [ "$UBUNTU_OS" == "" ]; then
        echo "-f, --force:Allow compile without virtual environment"jjj
    fi
    echo "html, blank:Compile to html"
    echo "latexpdf:   Compile to pdf"
    echo "packages:   Check for installed packages"
    echo
    exit 0
fi

# This is stupid, but sometimes numpy chokes on these files.
if [[ -f ../core/*.pyc ]]; then
    rm ../core/*.pyc
fi

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
pushd $SCRIPTPATH > /dev/null

if [ "$UBUNTU_OS" == "" ]; then
    # This is not Ubuntu, so look for a virtual environment
    if [[ -f /usr/lib/IntelAnalytics/virtpy/bin/activate ]]; then
        ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate
    else
        ACTIVATE_FILE=/usr/local/virtpy/bin/activate
    fi

    if [[ -f $ACTIVATE_FILE ]]; then
        source $ACTIVATE_FILE > /dev/null
    else
        echo "$1 $2 $3 $4 $5 $6 $7 $8 $9" | grep -i -e "-f" > /dev/null
        if [ $? == 1 ]; then
            echo "Virtual Environment is not installed."
            echo "Please execute install_pyenv.sh to install."
            popd > /dev/null
            exit 1
        fi
    fi
fi

# Look for packages if the individual is a techwriter
echo "$1 $2 $3 $4 $5 $6 $7 $8 $9" | grep -i "packages" > /dev/null
if [ $? == 0 ]; then
    # Yes for "packages", check for permissions
    if [ -f techwriters ]; then
        echo $USERNAME | grep --file=techwriters > /dev/null
        if [ $? == 0 ]; then
            clear
            # Check the installed packages and insure they are complete.
            # The packages need superuser rights to install properly.
            sudo $SCRIPTPATH/install_packages.sh
        fi
    fi
fi

# Look for html
COMPILE_HTML="Yes"
if [ "$#" -ne  "0" ]; then
    echo "$1 $2 $3 $4 $5 $6 $7 $8 $9" | grep -i "html" > /dev/null
    if [ $? -ne 0 ]; then
        # No for "html"
        COMPILE_HTML="No"
    fi
fi

if [ "$COMPILE_HTML" == "Yes" ]; then
    if [[ -d build/html ]]; then
        rm -r build/html
    fi
    # Ignore all toctree warnings.
    make -B html 2>&1 | grep -v -f toctreeWarnings
fi

# Look for latexpdf
echo "$1 $2 $3 $4 $5 $6 $7 $8 $9" | grep -i "latexpdf" > /dev/null
if [ $? == 0 ]; then
    if [[ -d build/latex ]]; then
        rm -r build/latex
    fi
    # Yes for "latexpdf"
    make -B latexpdf 2>&1 | grep -v -f toctreeWarnings
fi
# Look for text
echo "$1 $2 $3 $4 $5 $6 $7 $8 $9" | grep -i "text" > /dev/null
if [ $? == 0 ]; then
    if [[ -d build/text ]]; then
        rm -r build/text
    fi
    # Yes for "text"
    make -B text 2>&1 | grep -v -f toctreeWarnings
fi

echo Zipping Results...
zip -q -9 -r intel_analytics_docs build/*

exit 0
