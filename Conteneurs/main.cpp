#include <iostream>
#include <ctime>
#include "Conteneurs.hpp"

#define SAMPLE_SIZE 10000
using namespace std;

int main(int argc, char** argv) {
	clock_t t;

	vector<vector<int> > sample(SAMPLE_SIZE);
	for (int x = 0; x < SAMPLE_SIZE; ++x) {
		for (int y = 0; y < 4; ++y) {
			sample[x].push_back(y*x);
		}
	}

	cout << "Test de ConteneurNaif :" << endl;
	cout.flush();

	t = clock();
	ConteneurNaif cn(sample);
	t = clock() - t;

	cout << "Temps de creation :                        " << t << " ticks." << endl;
	cout.flush();

	t = clock();
	for (int x = 0; x < SAMPLE_SIZE; ++x) {
		for (int y = 0; y < 4; ++y)
			cn.get(x, y)++;
	}
	t = clock() - t;

	cout << "Temps pour incrementer tous les elements : " << t << " ticks." << endl << endl;
	cout.flush();


	cout << "Test de ConteneurBogoss :" << endl;
	cout.flush();

	t = clock();
	ConteneurBogoss cb(sample);
	t = clock() - t;

	cout << "Temps de creation :                        " << t << " ticks." << endl;
	cout.flush();

	t = clock();
	for (int x = 0; x < SAMPLE_SIZE; ++x) {
		for (int y = 0; y < 4; ++y) {
			try {
				cb.get(x, y)++;
			}
			catch (exception e) {
				cerr << "Arret sur cb.get pour (x,y) = (" << x << ", " << y << ")." << endl;
				cerr.flush();
				return -1;
			}
		}
	}
	t = clock() - t;

	cout << "Temps pour incrementer tous les elements : " << t << " ticks." << endl << endl;
	cout.flush();

	//system("pause"); for Windows, to stop window from closing immediately
	return 0;
}
