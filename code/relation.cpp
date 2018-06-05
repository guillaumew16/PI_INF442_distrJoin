#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <algorithm> // std::sort
#include <stdexcept>

#include "relation.hpp"
//#include "permutation.hpp" <-- already done

using namespace std;

Relation::Relation(int r) {
	this->r = r;
	z.reserve(r);
	for (int i=0; i<r; i++) { //z defaults to identity
		z.push_back(i);
	}
}

Relation::Relation(const char *filename) {
	//"won't-fix" bug:
	//chez moi quand on donne un filename qui n'existe pas, le constructor de ifstream ne renvoie pas d'exception et on obtient une Relation vide.

	cout << "Importing relation from file " << filename << ", assuming it is in correct format. Guessing arity while reading file." << endl;

	ifstream file(filename);
	string line;

	//getline moves forward to 2nd line
	if (!getline(file, line)) {
		cout << "Warning: tried to import relation from empty file. Returning empty Relation" << endl;
		this->r = 0; //any defined value. bonus: z.size() = r

	} else {
		//processing first line and guessing arity
		unsigned int newValue = -1;
		istringstream iss1(line);
		int n1=0;
		entries.push_back(vector<unsigned int>());
		while (iss1 >> newValue) {
			n1++;
			entries[0].push_back(newValue);
		}
		cout << "Guess for arity of " << filename << ": arity=" << n1 << endl;
		
		//initialize r and z
		this->r = n1;
		z.reserve(r);
		for (int i=0; i<r; i++) { //z defaults to identity
			z.push_back(i);
		}

		//get further lines
		while (getline(file, line)) {
			istringstream iss(line);
			entries.push_back(vector<unsigned int>());
			while (iss >> newValue) {
				entries.back().push_back(newValue);
			}
			if (entries.back().size() != r) {
				throw invalid_argument("Tried to import relation from file and guessing arity, but file is not in correct format");
			}
		}
	}

	cout << "Finished importing" << endl;
	file.close();
}

Relation::Relation(const char *filename, int r) {
	//"won't-fix" bug: (same as Relation(const char *filename))
	//chez moi quand on donne un filename qui n'existe pas, le constructor de ifstream ne renvoie pas d'exception et on obtient une Relation vide.

	cout << "Importing relation from file " << filename << ", *assuming* it is in correct format and has specified arity (" << r << ")" << endl;

	this->r = r;
	z.reserve(r);
	for (int i=0; i<r; i++) { //z defaults to identity
		z.push_back(i);
	}

	ifstream file(filename);
	unsigned int newValue = -1;
	int n=0;
	while (file >> newValue) {
		if (n == 0) {
			entries.push_back(vector<unsigned int>(r));
		}
		entries.back()[n] = newValue;

		n++;
		if (n == r) { //we could have used n%r instead, but file may be very long, so this is easier
			n = 0;
		}
	}

	if (entries.size() == 0) { //for consistency with behaviour of Relation(const char *)

	}
	
	/* for record, the old code for binary relations only:
	unsigned int a, b;
	while (file >> a >> b) {
		entries.push_back(vector<unsigned int>(2));
		entries.back()[0] = a;
		entries.back()[1] = b;
	}
	*/

	cout << "Finished importing" << endl;
	file.close();
}

Relation::Relation(const Relation &thatRel) {
	this->r = thatRel.getArity();
	this->z = thatRel.getVariables();
	this->entries = thatRel.getEntries();
}

// Relation::~Relation() {} //use the default destructor, which (recursively) calls destructor on each member

int Relation::getArity() const {
	return r;
}

vector<int> Relation::getVariables() const {
	return z;
}

int Relation::getVariable(int i) const {
	if (i >= r)
		throw invalid_argument("tried to getVariable for an index >= r");
	return z[i];
}

void Relation::setVariables(vector<int> &newZ) {
	if (newZ.size() != this->r)
		throw invalid_argument("tried to set z (list of variables) to a vector of size != r");

	//check that newZ has no duplicate variable (see README for reason)
	for (int i=0; i<r; i++) {
		for (int j=i+1; j<r; j++) {
			if (newZ[i] == newZ[j]) {
				throw invalid_argument("tried to set z (list of variables) to a newZ containing duplicate variables");
			}
		}
	}

	this->z = newZ;
}

vector<vector<unsigned int>> Relation::getEntries() const {
	return entries;
}

vector<unsigned int> Relation::getEntry(int i) const {
	if (i >= entries.size())
		throw invalid_argument("tried to getEntry for an index >= entries.size()");
	return entries[i];
}

vector<vector<unsigned int> >::iterator Relation::getBegin() {
	return entries.begin();
}

vector<vector<unsigned int> >::iterator Relation::getEnd() {
	return entries.end();
}

int Relation::getSize() const {
	return entries.size();
}

void Relation::addEntry(const vector<unsigned int> newEntry) {
	if (newEntry.size() != this->r)
		throw invalid_argument("received newEntry of size != r");

	entries.push_back(newEntry); //push_back creates a copy of the argument and stores the copy in the vector
}

void Relation::head(int nl) {
	if (entries.empty()){
		cout << "no entries" << endl;
		return;
	}
	int n=0;
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		n++;
		cout << "entry #" << n << ": ";
		for (int i=0; i < r-1; i++) {
			cout << (*it)[i] << " ";
		}
		cout << (*it)[r-1] << endl;

		if (n == nl) { //we print all entries if nl<0
			return;
		}
	}
}

void Relation::writeToFile(const char *filename)
{
	cout << "Writing relation to file " << filename << endl;

	ofstream file(filename);
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		for (int i=0; i < r-1; i++) {
			file << (*it)[i] << " ";
		}
		file << (*it)[r-1] << endl;
	}

	cout << "Finished writing to file" << endl;
	file.close();
}

void Relation::lexicoSort(Permutation &permut) {
	if (permut.getDimension() != this->r)
		throw invalid_argument("received permutation of dimension != r");
	
	vector<int> permut_descriptor = permut.getPermut();
	
	sort(entries.begin(), entries.end(), 
		[permut_descriptor](vector<unsigned int> const &a, vector<unsigned int> const &b) { return lexicoCompare(a, b, permut_descriptor); });

	/*alternatively we could have done "radix sort" using stable_sort. slightly less efficient due to stable_sort I think
	for (int i=0; i<r; i++){
		stable_sort(entries.begin(), entries.end(),
			 [i, permut](vector<unsigned int> const &a, vector<unsigned int> const &b) { return a[permut[i]] < b[permut[i]]; });
	}*/
}

void Relation::formatTriangle() {
	cout << "calling Relation::formatTriangle(). This will have side effects breaking the caller Relation!! Make sure you don't reuse the caller." << endl;
	
	if (r != 3)
		throw invalid_argument("called formatTriangle on a relation with arity != 3");
	
	if (entries.size() == 0) {
		return;
	}

	//sort each relation
	for (vector<vector<unsigned int> >::iterator it=entries.begin(); it!=entries.end(); it++) {
		sort((*it).begin(), (*it).end());
	}
	
	//sort entries
	Permutation identity(r);
	lexicoSort(identity);

	//remove identical entries
	vector<vector<unsigned int> > newEntries;
	newEntries.push_back(entries[0]);
	for (int i=1; i<entries.size(); i++) {
		//operator==: "lhs == rhs" checks if lhs.size() == rhs.size() and if each element in lhs compares equal with the element in rhs at the same position.
		if (entries[i] != entries[i-1]) {
			newEntries.push_back(entries[i]);
		}
	}
	entries = newEntries;
}

bool lexicoCompare(const vector<unsigned int> &e1, const vector<unsigned int> &e2) {
	//equivalent to lexicoCompare(e1, e2, identity_permutation)

	if (e1.size() != e2.size())
		throw invalid_argument("tried to compare entries of different dimensions");
	
	for (int i=0; i<e1.size(); i++) {
		if (e1[i] < e2[i]) {
			return true;
		} else if (e1[i] > e2[i]) {
			return false;
		}
	}
	return false;
}

bool lexicoCompare(const vector<unsigned int> &e1, const vector<unsigned int> &e2, vector<int> permutVect) {
	//can't pass permutVect as reference
	//must pass by copy to guarantee the lambda function in lexicoSort(...) that permut_descriptor is const (see lexicoSort)

	if (e1.size() != e2.size())
		throw invalid_argument("tried to compare entries of different dimensions");
	if (permutVect.size() != e1.size())
		throw invalid_argument("called lexicoCompare with a permutVect of dimension != dimension of entries to compare");

	for (int i = 0; i<e1.size(); i++) {
		if (e1[permutVect[i]] < e2[permutVect[i]]) {
			return true;
		}
		else if (e1[permutVect[i]] > e2[permutVect[i]]) {
			return false;
		}
	}
	return false;
}

void printVector(vector<int> v, const char *name) {
	cout << "Printing vector<int> " << name << ": " << endl;
	for (vector<int>::iterator it=v.begin(); it!=v.end(); it++){
		cout << *it << ' ';
	}
	cout << endl;
}

void printVector(vector<unsigned int> v, const char *name) {
	cout << "Printing vector<unsigned int> " << name << ": " << endl;
	for (vector<unsigned int>::iterator it=v.begin(); it!=v.end(); it++){
		cout << *it << ' ';
	}
	cout << endl;
}

Relation fancyImport(string name, string defaultPath) { //parameter default: defaultPath=""
	string filePath;
    string rStr;
    string zStr;
	int r;
	Relation rel(1);

	if (defaultPath != "") {
		cout << "data file for relation " << name << " (or leave empty to import from " << defaultPath << "): " << endl;
	} else {
		cout << "data file for relation " << name << ": " << endl;
	}
    getline(cin, filePath);

    cout << "arity of relation " << name << " (or leave empty to guess): " << endl;
    getline(cin, rStr);
    cout << "list of variables for relation " << name << ": " << endl;
    getline(cin, zStr);

	if (filePath == "") {
		if (defaultPath != "") {
			filePath = defaultPath;
		} else {
			cout << "you did not enter a data file path. Aborting" << endl;
			exit(1);
		}
	}
    if (rStr != "") {
        r = stoi(rStr);
        rel = Relation(filePath.c_str(), r);
    } else {
        rel = Relation(filePath.c_str());
        r = rel.getArity();
    }
    vector<int> z;
    istringstream iss(zStr);
    string word;
    while (iss >> word) {
        z.push_back(stoi(word));
    }
	if (z.size() != r) {
		cout << "given list of variables has wrong arity: " << z.size() << "!= expected arity: " << r <<". Will throw exception" << endl;
	}
    rel.setVariables(z);

	return rel;
}