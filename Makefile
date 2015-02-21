
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
		-lpdalcpp \
		-lcurl \
		-lcrypto \
		compression/*.cpp \
		http/*.cpp \
		kernel/*.cpp \
		tree/*.cpp \
		types/*.cpp \
		third/jsoncpp.cpp \
		-o entwine

