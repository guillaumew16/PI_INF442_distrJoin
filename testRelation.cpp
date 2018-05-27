//#include <stdlib.h>

#include "relation.hpp"
#include <iostream>

using namespace std;

int main(int argc, char** argv) {
	Relation myRel("data_head/twitter.dat");

	cout << "before sort:" << endl;
	myRel.head(10);

	vector<int> permutation(2);
	permutation[0]=1;
	permutation[1] = 0;
	cout << "sorting..." << endl;
	myRel.lexicoSort(permutation);

	cout << "before sort:" << endl;
	myRel.head(10);

	cout << "writing to file..." << endl;
	myRel.writeToFile("output.txt");

	return 0;
}
