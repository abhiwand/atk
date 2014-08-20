@echo off
echo /%1/%2/%3/%4/%5/%6/%7/%8/%9/ | find /I "/-h/" > nul
set E1=%ERRORLEVEL%
echo /%1/%2/%3/%4/%5/%6/%7/%8/%9/ | find /I "/--help/" > nul
set E2=%ERRORLEVEL%
set FH=0
if "%E1%=="1" set FH=1
 > /dev/null
if [ %? == 0 ]; then
    echo
    echo %0
    echo
    echo "-h, --help: Print this and exit"
    if [ "%UBUNTU_OS" == "" ]; then
        echo "-f, --force:Allow compile without virtual environment"
    fi
    echo "-z, --nozip:Do not zip up the contents"
    echo "zip:        Do a zip of build directory"
    echo "html:       Compile to html"
    echo "latex:      Compile to LaTeX"
    echo "latexonly:  Compile to LaTeX"
    echo "pdf:        Compile to pdf"
    echo "latexpdf:   Compile to pdf"
    echo "packages:   Check for installed packages"
    echo
    echo "File builds will erase previous files first. For example,"
    echo "if html is called for, this will delete the existing html files first."
    exit 0
fi
exit 0
# Subroutines
function make_backup () {
    FILENAME=%1
    if [[ -f ../core/%FILENAME.bak ]]; then
        echo "The file ../core/%FILENAME.bak already exists."
        return 1
    fi
    mv ../core/%FILENAME.py ../core/%FILENAME.bak
    if [ "%?" -ne "0" ]; then
        echo "Could not move %FILENAME.py to %FILENAME.bak."
        return 1
    fi
    return 0
    }

function reset_backup () {
    FILENAME=%1
    if [[ -f ../core/%FILENAME.bak ]]; then
        mv ../core/%FILENAME.bak ../core/%FILENAME.py
        if [ "%?" -ne "0" ]; then
            echo "Could not move %FILENAME.bak to %FILENAME.py. Reset incomplete."
            return 1
        fi
    fi
    return 0
    }

function change_class_object () {
    FILENAME=%1
    sed 's/\(class [ 0-9a-zA-Z_]*(\)[ 0-9a-zA-Z_]*)/\1object)/' < ../core/%FILENAME.bak > ../core/%FILENAME.py
    }

# Look for help
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/-h/" -e "/--help/" > /dev/null
if [ %? == 0 ]; then
    echo
    echo %0
    echo
    echo "-h, --help: Print this and exit"
    if [ "%UBUNTU_OS" == "" ]; then
        echo "-f, --force:Allow compile without virtual environment"
    fi
    echo "-z, --nozip:Do not zip up the contents"
    echo "zip:        Do a zip of build directory"
    echo "html:       Compile to html"
    echo "latex:      Compile to LaTeX"
    echo "latexonly:  Compile to LaTeX"
    echo "pdf:        Compile to pdf"
    echo "latexpdf:   Compile to pdf"
    echo "packages:   Check for installed packages"
    echo
    echo "File builds will erase previous files first. For example,"
    echo "if html is called for, this will delete the existing html files first."
    exit 0
fi

# This is stupid, but sometimes numpy chokes on these files.
if [[ -f ../core/*.pyc ]]; then
    rm ../core/*.pyc
fi

# Running on Ubuntu, we don't need virtpy because Ubuntu is awesome!
UBUNTU_OS=%(cat /etc/*-release | grep -i ubuntu)

SCRIPT=%(readlink -f "%0")
SCRIPTPATH=%(dirname "%SCRIPT")
pushd %SCRIPTPATH > /dev/null

if [ "%UBUNTU_OS" == "" ]; then
    # This is not Ubuntu, so look for a virtual environment
    if [[ -f /usr/lib/IntelAnalytics/virtpy/bin/activate ]]; then
        ACTIVATE_FILE=/usr/lib/IntelAnalytics/virtpy/bin/activate
    else
        ACTIVATE_FILE=/usr/local/virtpy/bin/activate
    fi

    if [[ -f %ACTIVATE_FILE ]]; then
        source %ACTIVATE_FILE > /dev/null
    else
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/-f/" -e "/--force/"> /dev/null
        if [ %? == 1 ]; then
            echo "Virtual Environment is not installed."
            echo "Please execute install_pyenv.sh to install."
            popd > /dev/null
            exit 1
        fi
    fi
fi

# Look for packages if the individual is a techwriter
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i "/packages/" > /dev/null
if [ %? == 0 ]; then
    # Yes for "packages", check for permissions
    if [ -f techwriters ]; then
        echo %USERNAME | grep --file=techwriters > /dev/null
        if [ %? == 0 ]; then
            clear
            echo Check the installed packages and insure they are complete.
            # The packages need superuser rights to install properly.
            sudo %SCRIPTPATH/install_packages.sh
            if [ %? == 0 ]; then
                exit 1
            fi
        fi
    fi
fi

# Look for something that builds
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/html/" -e "/latexonly/" -e "/latex/" -e "/latexpdf/" -e "/pdf/" -e "/text/" -e "/zip/" \
    > /dev/null
if [ %? == 0 ]; then
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/html/" -e "/latexonly/" -e "/latex/" -e "/latexpdf/" -e "/pdf/" -e "/text/" \
        > /dev/null
    if [ %? == 0 ]; then
        # Look for problems
        BAK_FOUND='NO'
        if [[ -f ../core/files.bak ]]; then
            BAK_FOUND='YES'
        fi
        if [[ -f ../core/frame.bak ]]; then
            BAK_FOUND='YES'
        fi
        if [[ -f ../core/graph.bak ]]; then
            BAK_FOUND='YES'
        fi
        if [ "%BAK_FOUND" == "YES" ]; then
            echo "Found a .bak for files, frame, or graph. Find out why."
            exit 1
        fi
        make_backup 'files'
        if [ "%?" -ne "0" ]; then
            exit 1
        fi
        make_backup 'frame'
        if [ "%?" -ne "0" ]; then
            reset_backup 'files'
            exit 1
        fi
        make_backup 'graph'
        if [ "%?" -ne "0" ]; then
            reset_backup 'files'
            reset_backup 'frame'
            exit 1
        fi

        # Translate each file to doc-specific file
        change_class_object 'files'
        change_class_object 'frame'
        change_class_object 'graph'

        # Look for html
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i "/html/" > /dev/null
        if [ %? == 0 ]; then
            if [[ -d build/html ]]; then
                rm -r build/html/*
            fi
            # Ignore all toctree warnings.
            make -B html 2>&1 | grep -v -f toctreeWarnings
        fi

        # Look for latexpdf
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/pdf/" -e "/latexpdf/" > /dev/null
        if [ %? == 0 ]; then
            if [[ -d build/latex ]]; then
                rm -r build/latex/*
            fi
            # Yes for "latexpdf"
            make -B latex 2>&1 | grep -v -f toctreeWarnings
            if [[ -f build/latex/IntelAnalytics.tex ]]; then
                python fix_latex.py
                if [ "%?" == "0" ] ; then
                    cd build/latex
                    make
                    cd ../..
                fi
            fi
        fi

        # Look for latex
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/latex/" -e "/latexonly/" > /dev/null
        if [ %? == 0 ]; then
            if [[ -d build/latex ]]; then
                rm -r build/latex/*
            fi
            # Yes for "latexpdf"
            make -B latex 2>&1 | grep -v -f toctreeWarnings
        fi
        # Look for text
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i "/text/" > /dev/null
        if [ %? == 0 ]; then
            if [[ -d build/text ]]; then
                rm -r build/text/*
            fi
            # Yes for "text"
            make -B text 2>&1 | grep -v -f toctreeWarnings
        fi

        # Restore each original file
        reset_backup 'files'
        reset_backup 'frame'
        reset_backup 'graph'

    fi

    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | grep -i -e "/-z/" -e "/--nozip/" > /dev/null
    if [ %? == 1 ]; then
        echo Zipping Results...
        zip -q -9 -r intel_analytics_docs build/*
    fi

fi
exit 0

