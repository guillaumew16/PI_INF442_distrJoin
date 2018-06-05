#CC = g++
CC = g++ -std=c++11 -g #-std=c++11: for lambda functions; -g: for debugging with gdb
MPICC = mpic++ -std=c++11 -g

all: demo test MPItest

demo: relation.o permutation.o join.o MPIjoin.o MurmurHash3.o demo.cpp
	$(MPICC) relation.o permutation.o join.o MPIjoin.o MurmurHash3.o demo.cpp -o demo

test: relation.o permutation.o join.o test.cpp
	$(CC) relation.o permutation.o join.o test.cpp -o test

MPItest: relation.o permutation.o join.o MPIjoin.o MurmurHash3.o MPItest.cpp
	$(MPICC) relation.o permutation.o join.o MPIjoin.o MurmurHash3.o MPItest.cpp -o MPItest

relation.o: relation.cpp relation.hpp
	$(CC) -c relation.cpp

permutation.o: permutation.cpp permutation.hpp
	$(CC) -c permutation.cpp

join.o: join.cpp join.hpp
	$(MPICC) -c join.cpp

MPIjoin.o: MPIjoin_nfs.cpp MPIjoin.hpp
	$(MPICC) -c MPIjoin_nfs.cpp -o MPIjoin.o

#MPIjoin.o: MPIjoin_copydata.cpp MPIjoin.hpp
#	$(MPICC) -c MPIjoin_copydata.cpp -o MPIjoin.o

#builds the .o in the current directory
MurmurHash3.o: hash_function/MurmurHash3.cpp hash_function/MurmurHash3.h
	$(CC) -c hash_function/MurmurHash3.cpp

clean:
	rm *.o
	rm test
	rm demo
	rm MPItest
	#rm ../output/*

cleanout:
	rm -r ../output/*
