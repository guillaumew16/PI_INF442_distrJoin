#ifndef PERMUTATION_H
#define PERMUTATION_H

#include <vector>

using namespace std;

class Permutation {

public:
	Permutation(int r); //identity
	Permutation(vector<int> toImport); //import from vector<int>
	//~Permutation(); <-- we use default destructor

    int operator[](int i); //try to avoid bugs by outputting a warning if trying to [] a Permutation instead of a vector<int>

	vector<int> getPermut() const; //read-only
	int getDimension() const; //read-only

    Permutation inverse();

    vector<unsigned int> permute(vector<unsigned int> entry) const;

private:
	int r; //dimension
	vector<int> permut; //guaranteed to be a permutation by Permutation constructors

};

bool isPermutation(vector<int> candidate);

#endif //PERMUTATION_H
