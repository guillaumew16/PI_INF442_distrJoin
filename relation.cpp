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
	if (newEntry.size() != this->r) {
		throw invalid_argument("received newEntry of size != r");
	}
	entries.push_back(newEntry);
}

vector<vector<unsigned int>> Relation::getEntries() const {
	return entries;
}

int Relation::getArity() const {
	return r;
}

void Relation::head(int nl) {
	if (entries.empty()){
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
	if (permutation.size() != this->r)
		throw invalid_argument("received permutation of size != r");
	if (!isPermutation(permutation))
		throw invalid_argument("received input which is not a permutation");

	//"radix sort".
	//alternatively we could have directly overloaded < operator
	//bool operator<(const Relation &that) const { return ...; }
	//and used sort(entries.begin(), entries.end());
	for (int i=0; i<r; i++){
		stable_sort(entries.begin(), entries.end(),
			 [i](vector<unsigned int> const &a, vector<unsigned int> const &b) { return a[i] < b[i]; });
	}
}

bool isPermutation(vector<int> permutation) {
	vector<bool> check(permutation.size(), false);
	for (int i=0; i<permutation.size(); i++) {
		if (check[permutation[i]]) {
			return false;
		}
		check[permutation[i]] = 1;
	}
	return true;
}

Relation join(Relation rel, vector<int> z, Relation relp, vector<int> zp){
	if (rel.getArity() != z.size())
		throw invalid_argument("First relation (rel) has arity != size of first list of variables (z)");
	if (relp.getArity() != zp.size())
		throw invalid_argument("Second relation (relp) has arity != size of second list of variables (zp)");

	//assuming there exists a global indexation of variables, x_i, z[i] is defined as: for given assignment a,
	//rel.getEntries()[a][i] = a(z[i])
	//idem for relp/zp. no assumption on order of z nor zp.

	vector<int> x;
	vector<int> permut;
	permut.reserve(rel.getArity());
	vector<int> permutp;
	permutp.reserve(relp.getArity());

	//will be useful to fill the rest of permut and permutp
	vector<bool> rest(rel.getArity(), true);
	vector<bool> restp(relp.getArity(), true);

	for (int i=0; i<z.size(); i++) {
		for (int j=0; j<zp.size(); j++) {
			if (z[i] == zp[j]){
				x.push_back(z[i]);
				permut.push_back(i);
				permutp.push_back(j);
				rest[i] = false;
				restp[j] = false;
				break;
			}
		}
	}

	for (int i=0; i<rest.size(); i++) {
		if (rest[i]) {
			permut.push_back(i);
		}
	}
	for (int i=0; i<restp.size(); i++) {
		if (restp[i]) {
			permutp.push_back(i);
		}
	}
	if (permut.size() != rel.getArity())
		throw length_error("size of permut after filling with rest does not match arity of rel");
	if (permutp.size() != relp.getArity())
		throw length_error("size of permutp after filling with restp does not match arity of relp");

	rel.lexicoSort(permut);
	relp.lexicoSort(permutp);

	rel.writeToFile("output/rel.txt");
	relp.writeToFile("output/relp.txt");

	return rel;
}
