
all: .FORCE

.FORCE:
	g++ -std=c++11 \
		-O1 \
		-Wno-deprecated-declarations \
		-Wall \
		-Werror \
		-pedantic \
		-fexceptions \
		-frtti \
		-g \
		-I. \
		-I./third \
		-I./third/json \
		-I/usr/include \
		compression/*.cpp \
		http/*.cpp \
		kernel/*.cpp \
		tree/*.cpp \
		types/*.cpp \
		third/jsoncpp.cpp \
		-lpdalcpp \
		-lcurl \
		-lcrypto \
		-o entwine

