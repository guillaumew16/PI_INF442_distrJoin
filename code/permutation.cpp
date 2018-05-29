#include <iostream>

#include "permutation.hpp"

using namespace std;

Permutation::Permutation(int r) {
    this->r = r;
    permut.reserve(r);
    for (int i=0; i<r; i++) {
        permut[i] = i;
    }
}

Permutation::Permutation(vector<int> toImport) {
    r = toImport.size();
    permut.reserve(r);

    if (!isPermutation(toImport))
        throw invalid_argument("tried to construct a Permutation by importing a vector which is not a permutation");

    for (int i=0; i<r; i++) {
        permut.push_back(toImport[i]);
    }
}

int Permutation::operator[](int i) {
    cout << "Warning: trying to [] a Permutation instead of a vector<int>. Change your code! Returning this->permut[-]" << endl;
    return permut[i];
}

vector<int> Permutation::getPermut() const {
    return permut;
}

int Permutation::getPermut(int i) const {
    if (i>=r)
        throw invalid_argument("tried to getPermut of an index >= r");
    return permut[i];
}

int Permutation::getDimension() const {
    return r;
}

Permutation Permutation::inverse() {
    vector<int> invPermut(r);
    for (int i=0; i<r; i++) {
        invPermut[permut[i]] = i;
    }
    return Permutation(invPermut);
}

vector<unsigned int> Permutation::permute(vector<unsigned int> entry) const {
    if (this->r != entry.size())
        throw invalid_argument("tried to permute an entry with dimension != permutation's dimension");

    vector<unsigned int> output(r);
    for (int i=0; i<r; i++) {
        output[i] = entry[permut[i]];
    }
    return output;
}

vector<int> Permutation::permute(vector<int> vect) const {
    if (this->r != vect.size())
        throw invalid_argument("tried to permute a vector<int> with dimension != permutation's dimension");

    vector<int> output(r);
    for (int i=0; i<r; i++) {
        output[i] = vect[permut[i]];
    }
    return output;
}

bool isPermutation(vector<int> candidate) {
	vector<bool> check(candidate.size(), false);
	for (int i=0; i<candidate.size(); i++) {
        if (candidate[i] >= candidate.size() || check[candidate[i]]) {
			return false;
		}
		check[candidate[i]] = true;
	}
	return true;
}
