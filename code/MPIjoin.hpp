#ifndef MPIJOIN_H
#define MPIJOIN_H

#include "relation.hpp"

int h(unsigned int tohash, int m); //hash function (% or murmurhash)

Relation MPIjoin(Relation &rel, Relation &relp, int root=0);

Relation MPIautoJoin(Relation &rel, vector<int> &zp, int root=0);

Relation MPItriangle(Relation &rel, int root=0);

#endif //MPIJOIN_H
