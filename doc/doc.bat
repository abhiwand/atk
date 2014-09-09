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
    echo -h, --help:    Print this and exit
    echo -e, --noerase: Do not erase previous compilation
    echo zip:           Do a zip of build directory
    echo html:          Compile to html
    echo latex:         Compile to LaTeX
    echo pdf:           Compile to pdf
    echo packages:      Check for installed packages
    echo -----------------------------------------
    echo Normally, file builds will erase previous files first. For example,
    echo if html is called for, this will delete the existing html files first.
    goto:fini
)
    
if "%DEBUG%" equ "0" echo : This is stupid, but sometimes numpy chokes on these files.
if exist %PY_PATH%\intelanalytics\core\*.pyc del %PY_PATH%\intelanalytics\core\*.pyc

:PACKAGES
if "%DEBUG%" equ "0" echo : Look for packages if the individual is a techwriter
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/packages/" > nul
if "%ERRORLEVEL%" neq "0" goto:END_PACKAGES
if "%DEBUG%" equ "0" echo : Yes for "packages", check for permissions
if not exist techwriters goto:END_PACKAGES
type techwriters | find /I %USERNAME% > nul
if "%ERRORLEVEL%" equ "1" goto:END_PACKAGES
echo Check the installed packages and insure they are complete.
if "%DEBUG%" equ "0" echo : The packages need superuser rights to install properly.
call install_packages.bat
if "%ERRORLEVEL%" neq "0" goto:fini
:END_PACKAGES

:HTML
if "%DEBUG%" equ "0" echo : Look for html
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/html/" > nul
if "%ERRORLEVEL%" equ "1" goto:END_HTML

:DELETE_HTML
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/-e/" > nul
if "%ERRORLEVEL%" equ "1" if exist build\html rd /S /Q build\html

:BUILD_HTML
if "%DEBUG%" equ "0" echo : Look for previous html directory
if "%DEBUG%" equ "0" echo : Ignore all toctree warnings.
sphinx-build -b html source build\html 2>&1 | find /V "WARNING: toctree"
if not exist build\latex goto:END_HTML
if exist build\latex\IntelAnalytics.pdf copy build\latex\IntelAnalytics.pdf build\html\_downloads > nul
if "%DEBUG%" equ "0" echo : Finished with HTML section
:END_HTML

:LATEX
if "%DEBUG%" equ "0" echo : Look for latex
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/latex/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
if "%BUILD%" equ "1" goto:END_LATEX

:DELETE_LATEX
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/-e/" > nul
if "%ERRORLEVEL%" equ "1" if exist build\html rd /S /Q build\latex

:BUILD_LATEX
if exist build\latex rd /s /q build\latex
sphinx-build -b latex source build\latex 2>&1 | find /V "WARNING: toctree"
:END_LATEX

:PDF
if not exist build\latex\IntelAnalytics.tex goto:END_PDF
if "%DEBUG%" equ "0" echo : Look for pdf
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
if "%BUILD%" equ "1" goto:END_PDF

:BUILD_PDF
rem python fix_latex.py
rem if "%ERRORLEVEL%" equ "1" goto:END_PDF
call tex-2-pdf %1 %2 %3 %4 %5 %6 %7 %8 %9
if not exist build\html goto:END_PDF
if exist build\latex\IntelAnalytics.pdf copy build\latex\IntelAnalytics.pdf build\html\_downloads > nul
:END_PDF

:TEXT
if "%DEBUG%" equ "0" echo : Look for text
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/text/" > nul
if "%ERRORLEVEL%" equ "1" goto:END_TEXT

:DELETE_TEXT
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/-e/" > nul
if "%ERRORLEVEL%" equ "1" if exist build\html rd /S /Q build\text

:BUILD_TEXT
if exist build\text rd /s /q build\text
if "%DEBUG%" equ "0" echo : Yes for "text"
sphinx-build -b text source build\text 2>&1 | find /V "WARNING: toctree"
:END_TEXT

:ZIP
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/zip/" > nul
if "%ERRORLEVEL%" equ "0" set BUILD=0
if "%BUILD%" equ "1" goto:END_ZIP

:BUILD_ZIP
echo Zipping Results...
zip -q -9 -r intel_analytics_docs build\*
:END_ZIP

:fini