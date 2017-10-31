@echo off

cmake -Wno-dev -G "NMake Makefiles" ^
    -DCMAKE_INSTALL_PREFIX=C:/OSGeo4W64 ^
    -DCMAKE_BUILD_TYPE=RelWithDebInfo ^
    -DBUILD_SHARED_LIBS=ON ^
    -Dgtest_force_shared_crt=ON .
