#CC = g++
CC = g++ -std=c++11 -g #-std=c++11: for lambda functions; -g: for debugging with gdb

all: test

test: relation.o permutation.o test.o
	$(CC) relation.o permutation.o test.o -o test

test.o: test.cpp
	$(CC) -c test.cpp

relation.o: relation.cpp relation.hpp
	$(CC) -c relation.cpp

permutation.o: permutation.cpp permutation.hpp
	$(CC) -c permutation.cpp

clean:
	rm -f *.o
	rm test
