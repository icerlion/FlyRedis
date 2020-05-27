# Generate by VCXProjToMake
# Cpp_Compiler
Cpp_Compiler=g++

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

# Include_Path
Include_Path=\
	-I"./include/" \
	-I"./boost_1_72_0/" \

Output_File=./publish/libFlyRedis.a

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
	mkdir -p ./build/
	mkdir -p ./publish/

# build of FlyRedis
.PHONE: build
build: init\
	./build/FlyRedis.o 
	@echo "|===>RunTarget: build of FlyRedis"
	ar rcs $(Output_File) \
	./build/FlyRedis.o 
	@echo "|===>Finish Output $(Output_File)"

# Compile cpp file ./include/FlyRedis/FlyRedis.cpp
-include ./build/FlyRedis.d
./build/FlyRedis.o: ./include/FlyRedis/FlyRedis.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c ./include/FlyRedis/FlyRedis.cpp -o ./build/FlyRedis.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM ./include/FlyRedis/FlyRedis.cpp > ./build/FlyRedis.d

# clean project output content
.PHONY: clean
clean: 
	@echo "|===>RunTarget: clean of FlyRedis"
	rm -rf ./build/*
	rm -rf $(Output_File)
