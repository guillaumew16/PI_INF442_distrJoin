#include <iostream>
#include <fstream>
#include <algorithm> // std::sort

#include "relation.hpp"
//#include "permutation.hpp" <-- already done

using namespace std;

Relation::Relation(int r) {
	this->r = r;
}

Relation::Relation(const char *filename) {
	cout << "Importing relation from file " << filename << ", assuming it is in correct format and has arity 2." << endl;
	cout << "To import a non-binary relation from a file, we will need to modify Relation's constructor." << endl;

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
		if (n == nl) { //we print all entries if nl<0
			return;
		}
	}
}

void Relation::writeToFile(const char *filename)
{
	cout << "Writing relation to file " << filename << ", assuming it has arity 2." << endl;
	cout << "To export a non-binary relation to a file, we will need to modify Relation::writeToFile(...)." << endl;

	ofstream file(filename);
	for (vector<vector<unsigned int>>::iterator it = entries.begin(); it != entries.end(); it++)
	{
		file << (*it)[0] << " " << (*it)[1] << endl;
	}
	file.close();
}

void Relation::lexicoSort(Permutation permut) {
	if (permut.getDimension() != this->r)
		throw invalid_argument("received permutation of dimension != r");
	
	sort(entries.begin(), entries.end(), 
		[permut](vector<unsigned int> const &a, vector<unsigned int> const &b) { return lexicoCompare(permut.permute(a), permut.permute(b)); });

	/*alternatively we could have done "radix sort" using stable_sort. slightly less efficient due to stable_sort I think
	for (int i=0; i<r; i++){
		stable_sort(entries.begin(), entries.end(),
			 [i, permut](vector<unsigned int> const &a, vector<unsigned int> const &b) { return a[permut[i]] < b[permut[i]]; });
	}*/
}

bool lexicoCompare(vector<unsigned int> e1, vector<unsigned int> e2) {
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

Relation join(Relation rel, vector<int> z, Relation relp, vector<int> zp){

	cout << "called join(...), but not finished coding" << endl;

	if (rel.getArity() != z.size())
		throw invalid_argument("First relation (rel) has arity != size of first list of variables (z)");
	if (relp.getArity() != zp.size())
		throw invalid_argument("Second relation (relp) has arity != size of second list of variables (zp)");

	//assuming there exists a global indexation of variables, v_j, z[i] is defined as: for given assignment a,
	//rel.getEntries()[a][i] = a(v_{z[i]})
	//idem for relp/zp. no assumption on order of z nor zp.

	vector<int> x;
	vector<int> permutVect;
	permutVect.reserve(rel.getArity());
	vector<int> permutpVect;
	permutpVect.reserve(relp.getArity());

	//will be useful to fill the rest of permut and permutp
	vector<bool> rest(rel.getArity(), true);
	vector<bool> restp(relp.getArity(), true);

	for (int i=0; i<z.size(); i++) {
		for (int j=0; j<zp.size(); j++) {
			if (z[i] == zp[j]){
				x.push_back(z[i]);
				permutVect.push_back(i);
				permutpVect.push_back(j);
				rest[i] = false;
				restp[j] = false;
				break;
			}
		}
	}

	for (int i=0; i<rest.size(); i++) {
		if (rest[i]) {
			permutVect.push_back(i);
		}
	}
	for (int i=0; i<restp.size(); i++) {
		if (restp[i]) {
			permutpVect.push_back(i);
		}
	}
	if (permutVect.size() != rel.getArity())
		throw length_error("size of permutVect after filling with rest does not match arity of rel");
	if (permutpVect.size() != relp.getArity())
		throw length_error("size of permutpVect after filling with restp does not match arity of relp");

	Permutation permut(permutVect);
	Permutation permutp(permutpVect);

	rel.lexicoSort(permut);
	relp.lexicoSort(permutp);

	/*
	//intermediary outputting for testing
	rel.writeToFile("output/rel.txt");
	relp.writeToFile("output/relp.txt");
	printVector(x, "x");
	printVector(permut, "permut");
	printVector(permutp, "permutp");
	*/

	// v-- BEGIN WORK
	vector<vector<unsigned int> >::iterator t_it = rel.getEntries().begin();
	vector<vector<unsigned int> >::iterator tp_it = relp.getEntries().begin();
/*
	if (coincide(*t_it, z, *tp_it, zp, x)) { //"t and tp coincide on x"
		//"add all tuples that agree with t and t' on x"
		cout << "t and tp coincide on x";
	}
*/
	return Relation(1);
}

bool coincide(vector<unsigned int> t, vector<int> z, vector<unsigned int> tp, vector<int> zp, vector<int> &x) {
	//"t and tp coincide on x"

	cout << "called coincide(...), but not finished coding" << endl;
	
	for (int i=0; i<x.size(); i++) {
		//not really efficient, but since z.size() = rel.getArity() = 2 in most applications, that's ok
		if (t[ *(find(z.begin(), z.end(), x[i])) ] != tp[ *(find(zp.begin(), zp.end(), x[i])) ]){
			return false;
		}
	}
	return true;
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
