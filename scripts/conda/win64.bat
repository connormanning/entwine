set CONDA_ENVIRO=entwine-build

del /s /q  %CONDA_ENVIRO%\*
mkdir %CONDA_ENVIRO%
pushd %CONDA_ENVIRO%

call conda config --set always_yes yes 
IF ERRORLEVEL 1 GOTO CLEANUP

call conda remove --name %CONDA_ENVIRO% -y --all
IF ERRORLEVEL 1 GOTO CLEANUP

call conda create --name %CONDA_ENVIRO% --clone pdal -y
IF ERRORLEVEL 1  GOTO CLEANUP


call %CONDA_PREFIX%\\Scripts\\activate.bat %CONDA_ENVIRO%
IF ERRORLEVEL 1 GOTO CLEANUP

call conda install jsoncpp
IF ERRORLEVEL 1 GOTO CLEANUP


REM set GENERATOR="Visual Studio 14 2015 Win64"
REM set GENERATOR="NMake Makefiles"
set GENERATOR="Ninja"


cmake -G %GENERATOR% ^
      -DCMAKE_BUILD_TYPE:STRING=RelWithDebInfo ^
      -DCMAKE_LIBRARY_PATH:FILEPATH="=%CONDA_PREFIX%/Library/lib" ^
      -DCMAKE_INCLUDE_PATH:FILEPATH="%CONDA_PREFIX%/Library/include" ^
      -DCMAKE_INSTALL_PREFIX:FILEPATH="%CONDA_PREFIX%" ^
      -DBUILD_SHARED_LIBS=ON ^
      -Dgtest_force_shared_crt=ON ^
      .. --debug-trycompile

call ninja
IF ERRORLEVEL 1 GOTO CLEANUP

:CLEANUP
call conda deactivate
popd
exit /b


