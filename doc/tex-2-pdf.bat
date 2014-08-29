@echo off
echo Processing LaTeX file...
rem Makefile for Sphinx LaTeX output
set DEBUG=1
cd build\latex

:BEGINNING
if "%1" equ "" goto:FINI

rem Prefix for archive names
set ARCHIVEPRREFIX=IA-
rem Additional LaTeX options
set LATEXOPTS=

goto:SKIP_LINUX

ALLDOCS = %(basename %(wildcard *.tex))
ALLPDF = %(addsuffix .pdf,%(ALLDOCS))
ALLDVI = %(addsuffix .dvi,%(ALLDOCS))


all: %(ALLPDF)
all-pdf: %(ALLPDF)
all-dvi: %(ALLDVI)
all-ps: all-dvi
:SKIP_LINUX

if "%DEBUG%" equ "0" echo : Convert dvi files to ps files
for %%f in (*.dvi) do dvips %%f

:ALL_PDF_JA
if "%DEBUG%" equ "0" echo : Check for ALL_PDF_JA to convert to Japanese
set BUILD=1
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/all_pdf_ja/" > nul
if "%ERRORLEVEL%"=="0" set BUILD=0
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/all-pdf-ja/" > nul
if "%ERRORLEVEL%"=="0" set BUILD=0
if "%BUILD%"=="1" goto:PDF
if "%DEBUG%" equ "0" echo : Processing for Japanese

if "%DEBUG%" equ "0" echo : Getting bounding box of all graphics
for %%f in (*.pdf *.png *.gif *.jpg *.jpeg) do extractbb %%f


for %%f in (*.tex) do platex -kanji=utf8 %LATEXOPTS% %%f
for %%f in (*.tex) do platex -kanji=utf8 %LATEXOPTS% %%f
for %%f in (*.tex) do platex -kanji=utf8 %LATEXOPTS% %%f
for %%f in (*.idx) do mendex -U -f -d "`basename %%f .idx`.dic" -s python.ist %%f
for %%f in (*.tex) do platex -kanji=utf8 %LATEXOPTS% %%f
for %%f in (*.tex) do platex -kanji=utf8 %LATEXOPTS% %%f
for %%f in (*.dvi) do dvipdfmx %%f

:PDF
if "%DEBUG%" equ "0" echo : Check for PDF
set BUILD="1"
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/pdf/" > nul
if "%ERRORLEVEL%"=="0" set BUILD="0"
if "%BUILD%"=="1" goto:ZIP
if "%DEBUG%" equ "0" echo : Processing for Pdf

for %%f in (*.tex) do (
    for /L %%i in (1,1,5) do echo r | pdflatex %LATEXOPTS% %%f > nul
)

:ZIP
if "%DEBUG%" equ "0" echo : Check for ZIP
set BUILD="1"
echo "/%1/%2/%3/%4/%5/%6/%7/%8/%9/" | find /I "/zip/" > nul
if "%ERRORLEVEL%"=="0" set BUILD="0"
if "%BUILD%"=="1" toto:CLEAN
if "%DEBUG%" equ "0" echo : Processing for Zip

set FMT="ZIP"
mkdir %ARCHIVEPREFIX%docs-%FMT%
for %%f in (*.PDF) do copy %%f %ARCHIVEPREFIX%docs-%FMT%
echo Zipping PDFs...
zip -q -r -9 %ARCHIVEPREFIX%docs-%FMT%.zip %ARCHIVEPREFIX%docs-%FMT%
rd /S /Q %ARCHIVEPREFIX%docs-%FMT%


:CLEAN
rem del /F /Q *.dvi *.log *.ind *.aux *.toc *.syn *.idx *.out *.ilg *.pla

:FINI
dir *.pdf
cd ..\..