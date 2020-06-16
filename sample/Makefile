# Generate by VCXProjToMake
# Cpp_Compiler
Cpp_Compiler=g++

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

# Include_Path
Include_Path=\
	-I"../include/" \
	-I"../boost_1_72_0/" \

Output_File=./publish/sample

# PreCompile_Macro
PreCompile_Macro=-DGCC_BUILD -DLINUX -D_DEBUG -D_LIB 

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

# Library_Path
Library_Path=\
	-L"../publish/"\
	-L"../boost_1_72_0/stage/lib/"

# Link_Library_Static
Link_Library_Static=-Wl,-Bstatic -Wl,--start-group -lFlyRedis -lboost_thread -pthread -Wl,--end-group

# Link_Library_Dynamic
Link_Library_Dynamic=-Wl,-Bdynamic -Wl,--start-group -Wl,--end-group

.PHONY: entry
entry: build

# Creates the intermediate and output folders
.PHONY: init
init:
	@echo "|===>RunTarget: init of sample"
	mkdir -p ./build
	mkdir -p ./publish

# build of sample
.PHONE: build
build: init\
	./build/main.o
	@echo "|===>RunTarget: build of sample"
	g++ \
	./build/main.o  -lrt $(Library_Path) $(Link_Library_Static) $(Link_Library_Dynamic) -o $(Output_File)
	@echo "|===>Finish Output $(Output_File)"

# Compile cpp file main.cpp
-include ./build/main.d
./build/main.o: main.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c main.cpp -o ./build/main.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM main.cpp > ./build/main.d

# clean project output content
.PHONY: clean
clean: 
	@echo "|===>RunTarget: clean of sample"
	rm -rf ./build/*
	rm -rf $(Output_File)
