@echo off
set DEBUG=0
if "%DEBUG%" equ "0" echo : Check for help request
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/-h/" > nul
set E1=%ERRORLEVEL%
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/--help/" > nul
set E2=%ERRORLEVEL%
set FH=1

if "%E1%" equ "0" set FH=0
if "%E2%" equ "0" set FH=0

if "%DEBUG%" equ "0" echo : Did it find help? E1=%E1%, E2=%E2%, FH=%FH%
if "%FH%" equ "0" (
    echo -----------------------------------------
    echo -h, --help: Print this and exit
    echo -z, --nozip:Do not zip up the contents
    echo zip:        Do a zip of build directory
    echo html:       Compile to html
    echo latex:      Compile to LaTeX
    echo latexonly:  Compile to LaTeX
    echo pdf:        Compile to pdf
    echo latexpdf:   Compile to pdf
    echo packages:   Check for installed packages
    echo -----------------------------------------
    echo File builds will erase previous files first. For example,
    echo if html is called for, this will delete the existing html files first.
    goto:fini
)
    
goto:del_pyc

if "%DEBUG%" equ "0" echo : Subroutines
:make_backup
    if "%DEBUG%" equ "0" echo : Make a backup of the %PARAMETER% file
    set FILENAME=%PARAMETER%
    if exist ..\core\%FILENAME%.bak (
        echo The file ..\core\%FILENAME%.bak already exists.
        set RETURN_VALUE=1
        goto:%CALL_FROM%
    )
    move ..\core\%FILENAME%.py ..\core\%FILENAME%.bak
    if "%ERRORLEVEL%" NEQ "0" (
        echo Could not move %FILENAME%.py to %FILENAME%.bak.
        set RETURN_VALUE=1
        goto:%CALL_FROM%
    )
    set RETURN_VALUE=0
    goto:%CALL_FROM%

:reset_backup
    if "%DEBUG%" equ "0" echo : Reset a backup of the %PARAMETER% file
    set FILENAME=%PARAMETER%
    if exist ..\core\%FILENAME%.bak (
        copy ..\core\%FILENAME%.bak ..\core\%FILENAME%.py
        if "%ERRORLEVEL%" neq "0" (
            echo Could not copy %FILENAME%.bak to %FILENAME%.py. Reset incomplete.
            set RETURN_VALUE=1
            goto:%CALL_FROM%
        )
    )
    set RETURN_VALUE=0
    goto:%CALL_FROM%

:del_pyc
if "%DEBUG%" equ "0" echo : This is stupid, but sometimes numpy chokes on these files.
if exist ..\core\*.pyc del ..\core\*.pyc

if "%DEBUG%" equ "0" echo : Look for packages if the individual is a techwriter
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/packages/" > nul
if "%ERRORLEVEL%" equ "0" (
    if "%DEBUG%" equ "0" echo : Yes for "packages", check for permissions
    if exist techwriters (
        type techwriters | find /I %USERNAME% > nul
        if "%ERRORLEVEL%" equ "0" (
            cls
            echo Check the installed packages and insure they are complete.
            if "%DEBUG%" equ "0" echo : The packages need superuser rights to install properly.
            call install_packages.bat
            if "%ERRORLEVEL%" equ "0" goto:fini
        )
    )
)

if "%DEBUG%" equ "0" echo : Look for something that builds
set BUILDS=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/html/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexonly/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latex/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexpdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/text/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/zip/" > nul
if "%ERRORLEVEL%" equ "0" set BUILDS=0
echo Flag

if "%BUILDS%" equ "0" (
    if "%DEBUG%" equ "0" echo : Look for something that builds other than zip
    set BUILDS=1
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/html/" > nul
    if "%ERRORLEVEL%" equ "0" set BUILDS=0
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexonly/" > nul
    if "%ERRORLEVEL%" equ "0" set BUILDS=0
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latex/" > nul
    if "%ERRORLEVEL%" equ "0" set BUILDS=0
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexpdf/" > nul
    if "%ERRORLEVEL%" equ "0" set BUILDS=0
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
    if "%ERRORLEVEL%" equ "0" set BUILDS=0
    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/text/" > nul
    if "%ERRORLEVEL%" equ "0" set BUILDS=0
    if "%BUILDS%" equ "0" (
        
        goto:SKIP_PREPROCESS
        
        if "%DEBUG%" equ "0" echo : Pre-process the python files
        set BAK_FOUND=1
        if exist ..\core\files.bak set BAK_FOUND=0
        if exist ..\core\frame.bak set BAK_FOUND=0
        if exist ..\core\graph.bak set BAK_FOUND=0
        if "%BAK_FOUND%" equ "0" ( echo Found a .bak for files, frame, or graph. Find out why.; goto:fini )
        set PARAMETER=files
        set CALL_FROM=BACKUP_FILES
        goto make_backup
        :BACKUP_FILES
        if "%RETURN_VALUE%" neq "0" goto:fini
        set PARAMETER=frame
        set CALL_FROM=BACKUP_FRAME
        goto make_backup
        :BACKUP_FRAME
        if "%RETURN_VALUE%" neq "0" goto:fini
        set PARAMETER=graph
        set CALL_FROM=BACKUP_GRAPH
        goto make_backup
        :BACKUP_GRAPH
        if "%RETURN_VALUE%" neq "0" goto:fini
        
        :SKIP_PREPROCESS

        if "%DEBUG%" equ "0" echo : Look for html
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/html/" > nul
        if "%ERRORLEVEL%" equ "0" (
            if exist build\html (
                rd /S /Q build\html
            )
            if "%DEBUG%" equ "0" echo : Ignore all toctree warnings.
            sphinx-build -b html source build\html 2>&1 | find /V "WARNING: toctree"
        )
        if "%DEBUG%" equ "0" echo : Finished with HTML section
goto:fini
        if "%DEBUG%" equ "0" echo : Look for latexpdf
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I -e "/pdf/" -e "/latexpdf/" > nul
        if "%ERRORLEVEL%" equ "0" (
            if exist build\latex (
                rd /s /q build\latex\*
            )
            if "%DEBUG%" equ "0" echo : Yes for "latexpdf"
            sphinx-build -b latex 2>&1 | find /V "WARNING: toctree"
            if exist build\latex/IntelAnalytics.tex (
                python fix_latex.py
                if "%ERRORLEVEL%" equ "0" (
                    cd build\latex
                    echo make
                    cd ..\..
                )
            )
        )
goto:fini
        if "%DEBUG%" equ "0" echo : Look for latex
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I -e "/latex/" -e "/latexonly/" > nul
        if "%ERRORLEVEL%" equ "0" (
            if exist build\latex (
                rd /s /q build\latex\*
            )
            if "%DEBUG%" equ "0" echo : Yes for "latexpdf"
            sphinx-build -b latex 2>&1 | find /V "WARNING: toctree"
        )
        if "%DEBUG%" equ "0" echo : Look for text
        echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/text/" > nul
        if "%ERRORLEVEL%" equ "0" (
            if exist build\text (
                rd /s /q build\text\*
            )
            if "%DEBUG%" equ "0" echo : Yes for "text"
            sphinx-build -b text 2>&1 | find /V "WARNING: toctree"
        )

        if "%DEBUG%" equ "0" echo : Restore each original file
        reset_backup 'files'
        reset_backup 'frame'
        reset_backup 'graph'

    )

    echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I -e "/-z/" -e "/--nozip/" > nul
    if "%ERRORLEVEL%" equ "1" (
        echo Zipping Results...
        zip -q -9 -r intel_analytics_docs build\*
    )

)

:fini