@echo off
set DEBUG=1
if "%DEBUG%" equ "0" echo : Check for help request
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/-h/" > nul
set E1=%ERRORLEVEL%
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/--help/" > nul
set E2=%ERRORLEVEL%
set FH=1
set PY_PATH=C:\USERS\RMALDRIX\WORK\PYTHON

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
    
if "%DEBUG%" equ "0" echo : This is stupid, but sometimes numpy chokes on these files.
if exist %PY_PATH%\intelanalytics\core\*.pyc del %PY_PATH%\intelanalytics\core\*.pyc

if "%DEBUG%" equ "0" echo : Look for packages if the individual is a techwriter
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/packages/" > nul
if "%ERRORLEVEL%" neq "0" goto:CONCRETE
if "%DEBUG%" equ "0" echo : Yes for "packages", check for permissions
if not exist techwriters goto:CONCRETE
type techwriters | find /I %USERNAME% > nul
if "%ERRORLEVEL%" equ "1" goto:CONCRETE
echo Check the installed packages and insure they are complete.
if "%DEBUG%" equ "0" echo : The packages need superuser rights to install properly.
call install_packages.bat
if "%ERRORLEVEL%" neq "0" goto:fini

:CONCRETE
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

if "%BUILDS%" equ "1" goto:fini

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
if "%BUILDS%" equ "1" goto:ZIP_IT

:SEEK_HTML
if "%DEBUG%" equ "0" echo : Look for html
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/html/" > nul
if "%ERRORLEVEL%" equ "1" goto:SEEK_LATEX

:BUILD_HTML
if "%DEBUG%" equ "0" echo : Look for previous html directory
if exist build\html rd /S /Q build\html
if "%DEBUG%" equ "0" echo : Ignore all toctree warnings.
sphinx-build -b html source build\html 2>&1 | find /V "WARNING: toctree"
if not exist build\latex goto:SEEK_LATEX
if exist build\latex\IntelAnalytics.pdf copy build\latex\IntelAnalytics.pdf build\html\_downloads > nul
if "%DEBUG%" equ "0" echo : Finished with HTML section

:SEEK_LATEX
if "%DEBUG%" equ "0" echo : Look for latex
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexpdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latex/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexonly/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
if "%BUILD%" equ "1" goto:SEEK_TEXT

:BUILD_LATEX
if exist build\latex rd /s /q build\latex
sphinx-build -b latex source build\latex 2>&1 | find /V "WARNING: toctree"
if not exist build\latex\IntelAnalytics.tex goto:SEEK_TEXT

:SEEK_PDF
if "%DEBUG%" equ "0" echo : Look for pdf
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latexpdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
if "%BUILD%" equ "1" goto:SEEK_TEXT

:BUILD_PDF
python fix_latex.py
if "%ERRORLEVEL%" equ "1" goto:SEEK_TEXT
call tex-2-pdf %1 %2 %3 %4 %5 %6 %7 %8 %9
if not exist build\html goto:SEEK_TEXT
if exist build\latex\IntelAnalytics.pdf copy build\latex\IntelAnalytics.pdf build\html\_downloads > nul

:SEEK_TEXT
if "%DEBUG%" equ "0" echo : Look for text
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/text/" > nul
if "%ERRORLEVEL%" equ "1" goto:ZIP_IT

:BUILD_TEXT
if exist build\text rd /s /q build\text
if "%DEBUG%" equ "0" echo : Yes for "text"
sphinx-build -b text source build\text 2>&1 | find /V "WARNING: toctree"

:ZIP_IT
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/-z/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/--nozip/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
if "%BUILD%" equ "0" goto:fini
echo Zipping Results...
zip -q -9 -r intel_analytics_docs build\*

:fini