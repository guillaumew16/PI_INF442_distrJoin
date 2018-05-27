//#include <stdlib>
#include <iostream>
#include <fstream>
#include <algorithm> // std::sort

#include "relation.hpp"

using namespace std;

Relation::Relation(int r) {
	this->r = r;
}

Relation::Relation(const char *filename) {
	// assuming relation described in the file is binary
	this->r = 2;

	ifstream file(filename);
	unsigned int a, b;
	while (file >> a >> b) {
		entries.push_back(vector<unsigned int>(2));
		entries.back()[0] = a;
		entries.back()[1] = b;
	}
	file.close();
}

// Relation::~Relation() {} //use the default destructor, which (recursively) calls destructor on each member

void Relation::addEntry(vector<unsigned int> newEntry) {
	if (newEntry.size() != this->r)	{
		throw invalid_argument("received newEntry of size != r");
	}
	entries.push_back(newEntry);
}

vector<vector<unsigned int>> Relation::getEntries() const {
	return entries;
}

void Relation::head(int nl) {
	if (entries.size() == 0){
		cout << "no entries" << endl;
		return;
	}
	int n=0;
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		n++;
		cout << "entry #" << n << ": " << (*it)[0] <<" "<< (*it)[1] << endl;
		if (n == nl) {
			return;
		}
	}
}

void Relation::writeToFile(const char *filename)
{
	ofstream file(filename);
	for (vector<vector<unsigned int>>::iterator it = entries.begin(); it != entries.end(); it++)
	{
		file << (*it)[0] << " " << (*it)[1] << endl;
	}
	file.close();
}

void Relation::lexicoSort(vector<int> permutation) {
	if (permutation.size() != this->r) {
		throw invalid_argument("received permutation of size != r");
	}

	//"radix sort".
	//alternatively we could have directly overloaded < operator
	//bool operator<(const Relation &that) const { return ...; }
	//and used sort(entries.begin(), entries.end());
	for (int i=0; i<r; i++){
		stable_sort(entries.begin(), entries.end(),
			 [i](vector<unsigned int> const &a, vector<unsigned int> const &b) { return a[i] < b[i]; });
	}
}


