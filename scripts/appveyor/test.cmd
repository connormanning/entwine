@echo off

cd c:\projects\entwine\test
set PATH=%PATH%;c:\projects\entwine\test\gtest-1.8.0
set CURL_CA_BUNDLE=C:\OSGeo4W64\bin\curl-ca-bundle.crt
.\entwine-test.exe


