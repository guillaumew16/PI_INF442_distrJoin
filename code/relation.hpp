#ifndef RELATION_H
#define RELATION_H

#include <vector>
#include "permutation.hpp"

using namespace std;

class Relation {

public:
	Relation(int r); //empty Relation
	Relation(const char *filename, int r); //import from .dat
	//~Relation(); <-- we use default destructor

	void addEntry(vector<unsigned int> newEntry);
	vector<vector<unsigned int> > getEntries() const; //read-only
	vector<vector<unsigned int> >::iterator getBegin(); //can't force read-only due to nature of vector::iterator :(
	vector<vector<unsigned int> >::iterator getEnd();
	int getArity() const; //read-only
	
	void head(int nl); //print out first nl entries, or all entries if nl<0
	void writeToFile(const char *filename);

	void lexicoSort(Permutation permutation);

private:
	int r; //arity
	vector<vector<unsigned int> > entries; //guaranteed to hold entries of arity r by constructors and addEntry

};

bool lexicoCompare(vector<unsigned int> e1, vector<unsigned int> e2); //return "e1 < e2"

Relation join(Relation rel, vector<int> z, Relation relp, vector<int> zp);
//auxiliary functions for join:
vector<unsigned int> pi_x(vector<unsigned int> t, Permutation permut, int c);
bool coincide(vector<unsigned int> t, Permutation permut, vector<unsigned int> tp, Permutation permutp, int c);
bool agree(vector<unsigned int> s, vector<unsigned int> t, Permutation permut, int c);
vector<unsigned int> mergeEntry(vector<unsigned int> t, Permutation permut, vector<unsigned int> tp, Permutation permutp, int c);

void printVector(vector<int> v, const char *name); //for testing purposes
void printVector(vector<unsigned int> v, const char *name);

#endif //RELATION_H
