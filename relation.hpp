#ifndef RELATION_H
#define RELATION_H

#include <vector>

using namespace std;

class Relation {

public:
	Relation(int r); //empty Relation
	Relation(const char *filename); //import from .dat
	//~Relation(); <-- we use default destructor

	void addEntry(vector<unsigned int> newEntry);
	vector<vector<unsigned int> > getEntries() const; //read-only
	
	void head(int nl); //print out first nl entries
	void writeToFile(const char *filename);

	void lexicoSort(vector<int> permutation);

  private:
	int r; //arity
	vector<vector<unsigned int> > entries;

};

#endif //RELATION_H
