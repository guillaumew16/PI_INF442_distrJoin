CC = g++ -g #-g for debugging with gdb

all: main

main: Conteneurs.o main.o
	$(CC) Conteneurs.o main.o -o main

main.o: main.cpp
	$(CC) -c main.cpp

Conteneurs.o: Conteneurs.cpp Conteneurs.hpp
	$(CC) -c Conteneurs.cpp

clean:
	rm *.o
	rm main
