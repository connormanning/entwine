@echo off

cmake -Wno-dev -G "Visual Studio 14 2015 Win64" ^
    -DCMAKE_INSTALL_PREFIX=C:\pdalbin 	^
    -DBUILD_SHARED_LIBS=ON ^
    -Dgtest_force_shared_crt=ON .