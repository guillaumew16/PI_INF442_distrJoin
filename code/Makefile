#CC = g++
CC = g++ -std=c++11 -g #-std=c++11: for lambda functions; -g: for debugging with gdb

all: demo test

demo: relation.o permutation.o demo.o MurmurHash3.o
	$(CC) relation.o permutation.o MurmurHash3.o demo.o -o demo
demo.o: demo.cpp
	$(CC) -c demo.cpp

test: relation.o permutation.o test.o MurmurHash3.o
	$(CC) relation.o permutation.o MurmurHash3.o test.o -o test
test.o: test.cpp
	$(CC) -c test.cpp

relation.o: relation.cpp relation.hpp
	$(CC) -c relation.cpp

permutation.o: permutation.cpp permutation.hpp
	$(CC) -c permutation.cpp

#builds the .o in the current directory
MurmurHash3.o: hash_function/MurmurHash3.cpp hash_function/MurmurHash3.h
	$(CC) -c hash_function/MurmurHash3.cpp

clean:
	rm *.o
	rm test
	rm demo
	#rm ../output/*

cleanout:
	rm ../output/*
