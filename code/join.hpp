#ifndef JOIN_H
#define JOIN_H

#include <vector>
#include "relation.hpp"
#include "permutation.hpp"

using namespace std;

Relation join(Relation &rel, Relation &relp, int verbose = 1);

//auxiliary functions for join:
vector<unsigned int> pi_x(vector<unsigned int> &t, Permutation &permut, int c);
bool coincide(vector<unsigned int> &t, Permutation &permut, vector<unsigned int> &tp, Permutation &permutp, int c);
bool agree(vector<unsigned int> &s, vector<unsigned int> &t, Permutation &permut, int c);
vector<unsigned int> mergeEntry(vector<unsigned int> &t, Permutation &permut, vector<unsigned int> &tp, Permutation &permutp, int c);

Relation autoJoin(Relation &rel, vector<int> &zp); //equivalent to join(rel with given rel.z, rel with rel.z=zp)
Relation triangle(Relation &rel);

#endif //JOIN_H
