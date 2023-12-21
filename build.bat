@echo off

set DEBUG_MODE=

if "%1" == "debug" (
  set DEBUG_MODE=debug
)

cd net.frontuari.lvecustomprocess.targetplatform
call .\plugin-builder.bat %DEBUG_MODE% ..\net.frontuari.lvecustomprocess ..\net.frontuari.lvecustomprocess.test
cd ..
