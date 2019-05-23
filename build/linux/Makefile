# Generate by VCXProjToMake
# Cpp_Compiler
Cpp_Compiler=g++

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

# Include_Path
Include_Path=\
	-I"../../include/" \
	-I"/home/boost_1_70_0/" \

Output_File=../../publish/Debug/libFlyRedis.a

# PreCompile_Macro
PreCompile_Macro=-DGCC_BUILD -DLINUX -D_DEBUG -D_LIB 

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

.PHONY: entry
entry: build

# Creates the intermediate and output folders
.PHONY: init
init:
	@echo "|===>RunTarget: init of FlyRedis"
	mkdir -p ../../bin/Debug
	mkdir -p ../../publish/Debug

# build of FlyRedis
.PHONE: build
build: init\
	../../bin/Debug/FlyRedis.o \
	../../bin/Debug/FlyRedisClient.o \
	../../bin/Debug/FlyRedisSession.o
	@echo "|===>RunTarget: build of FlyRedis"
	ar rcs $(Output_File) \
	../../bin/Debug/FlyRedis.o \
	../../bin/Debug/FlyRedisClient.o \
	../../bin/Debug/FlyRedisSession.o 
	@echo "|===>Finish Output $(Output_File)"

# Compile cpp file ../../include/FlyRedis/FlyRedis.cpp
-include ../../bin/Debug/FlyRedis.d
../../bin/Debug/FlyRedis.o: ../../include/FlyRedis/FlyRedis.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c ../../include/FlyRedis/FlyRedis.cpp -o ../../bin/Debug/FlyRedis.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM ../../include/FlyRedis/FlyRedis.cpp > ../../bin/Debug/FlyRedis.d

# Compile cpp file ../../include/FlyRedis/FlyRedisClient.cpp
-include ../../bin/Debug/FlyRedisClient.d
../../bin/Debug/FlyRedisClient.o: ../../include/FlyRedis/FlyRedisClient.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c ../../include/FlyRedis/FlyRedisClient.cpp -o ../../bin/Debug/FlyRedisClient.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM ../../include/FlyRedis/FlyRedisClient.cpp > ../../bin/Debug/FlyRedisClient.d

# Compile cpp file ../../include/FlyRedis/FlyRedisSession.cpp
-include ../../bin/Debug/FlyRedisSession.d
../../bin/Debug/FlyRedisSession.o: ../../include/FlyRedis/FlyRedisSession.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c ../../include/FlyRedis/FlyRedisSession.cpp -o ../../bin/Debug/FlyRedisSession.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM ../../include/FlyRedis/FlyRedisSession.cpp > ../../bin/Debug/FlyRedisSession.d

# clean project output content
.PHONY: clean
clean: 
	@echo "|===>RunTarget: clean of FlyRedis"
	rm -rf ../../bin/Debug/*
	rm -rf $(Output_File)
