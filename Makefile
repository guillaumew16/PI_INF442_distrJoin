CC = g++ -std=c++11

all: testRelation

testRelation: relation.o testRelation.o
	$(CC) relation.o testRelation.o -o testRelation

testRelation.o: testRelation.cpp relation.cpp
	$(CC) -c testRelation.cpp

relation.o: relation.cpp relation.hpp
	$(CC) -c relation.cpp

clean:
	rm testRelation
	rm -f *.o

