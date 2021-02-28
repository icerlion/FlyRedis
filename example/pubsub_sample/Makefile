# Generate by VCXProjToMake
# Cpp_Compiler
Cpp_Compiler=g++

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

# Include_Path
Include_Path=\
	-I"../../include/" \
	-I"../../boost_1_72_0/" \

Output_File=./pubsub_sample

# PreCompile_Macro
PreCompile_Macro=-DGCC_BUILD -DLINUX -D_DEBUG -D_LIB -DFLY_REDIS_ENABLE_TLS

# Compiler_Flags
Compiler_Flags=-O0 -Wall -ggdb -std=c++11

# Library_Path
Library_Path=\
	-L"../../boost_1_72_0/stage/lib/"

# Link_Library_Static
Link_Library_Static=-Wl,-Bstatic -Wl,--start-group -lssl -lcrypto -lboost_thread -lboost_chrono -pthread -Wl,--end-group

# Link_Library_Dynamic
Link_Library_Dynamic=-Wl,-Bdynamic -Wl,--start-group -Wl,--end-group

.PHONY: entry
entry: build

# Creates the intermediate and output folders
.PHONY: init
init:
	@echo "|===>RunTarget: init of pubsub_sample"
	mkdir -p ./build
	mkdir -p ./publish

# build of pubsub_sample
.PHONE: build
build: init\
	./build/FlyRedis.o \
	./build/pubsub_sample.o
	@echo "|===>RunTarget: build of pubsub_sample"
	g++ \
	./build/FlyRedis.o \
	./build/pubsub_sample.o -lrt -ldl $(Library_Path) $(Link_Library_Static) $(Link_Library_Dynamic) -o $(Output_File)
	@echo "|===>Finish Output $(Output_File)"

# Compile cpp file pubsub_sample.cpp
-include ./build/pubsub_sample.d
./build/pubsub_sample.o: pubsub_sample.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c pubsub_sample.cpp -o ./build/pubsub_sample.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM pubsub_sample.cpp > ./build/pubsub_sample.d
	
# Compile cpp file FlyRedis.cpp
-include ./build/FlyRedis.d
./build/FlyRedis.o: ../../include/FlyRedis/FlyRedis.cpp
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -c ../../include/FlyRedis/FlyRedis.cpp -o ./build/FlyRedis.o
	$(Cpp_Compiler) $(Include_Path) $(PreCompile_Macro) $(Compiler_Flags) -MM ../../include/FlyRedis/FlyRedis.cpp > ./build/FlyRedis.d

# clean project output content
.PHONY: clean
clean: 
	@echo "|===>RunTarget: clean of pubsub_sample"
	rm -rf ./build/*
	rm -rf $(Output_File)
