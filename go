#!/usr/bin/env bash

g++ -std=c++11 \
    -Wno-deprecated-declarations \
    -Wall \
    -Werror \
    -pedantic \
    -fexceptions \
    -frtti \
    -I. \
    -I./third \
    -I./third/json \
    -lpdalcpp \
    -lcurl \
    -lcrypto \
    compression/*.cpp http/*.cpp kernel/*.cpp tree/*.cpp types/*.cpp third/jsoncpp.cpp \
    -o entwine

