#ifndef RELATION_H
#define RELATION_H

#include <vector>
#include "permutation.hpp"

using namespace std;

class Relation {

public:
	Relation(int r); //empty Relation
	Relation(const char *filename, int r); //import from .dat
	Relation(const Relation &thatRel); //copy an existing Relation
	//~Relation(); <-- we use default destructor

	int getArity() const; //read-only

	vector<int> getVariables() const; //read-only
	int getVariable(int i) const; //read-only
	void setVariables(vector<int> &newZ);

	vector<vector<unsigned int> > getEntries() const; //read-only
	vector<unsigned int> getEntry(int i) const;
	vector<vector<unsigned int> >::iterator getBegin(); //can't force read-only due to nature of vector::iterator :(
	vector<vector<unsigned int> >::iterator getEnd();
	int getSize() const;
	void addEntry(const vector<unsigned int> newEntry); //adds a copy of newEntry
	
	void head(int nl); //print out first nl entries, or all entries if nl<0
	void writeToFile(const char *filename);

	void lexicoSort(Permutation &permutation);

	void formatTriangle(); //avoid repeating entries when computing triangle (cf join.cpp and MPIjoin.cpp). breaks the Relation!

private:
	int r; //arity
	vector<int> z; //list of variables. defaults to identity, use setVariables(...) to modify
	//assuming there exists a global indexation of variables, v_j, z[i] is defined as: for given assignment (of variables) a,
	//rel.getEntries()[a][i] = a(v_{z[i]})
	vector<vector<unsigned int> > entries; //guaranteed to hold entries of arity r by constructors and addEntry

};

bool lexicoCompare(const vector<unsigned int> &e1, const vector<unsigned int> &e2); //return "e1 < e2" (equivalent to lexicoCompare(e1, e2, Identity(e1.size())) )
bool lexicoCompare(const vector<unsigned int> &e1, const vector<unsigned int> &e2, vector<int> permut); //return "e1 < e2" for variables order permutVect



void printVector(vector<int> v, const char *name); //for testing purposes
void printVector(vector<unsigned int> v, const char *name);

#endif //RELATION_H
