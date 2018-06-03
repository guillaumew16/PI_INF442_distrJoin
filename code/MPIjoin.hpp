#ifndef MPIJOIN_H
#define MPIJOIN_H

#include "relation.hpp"

int h(unsigned int tohash, int m, uint32_t seed=42); //hash function (% or murmurhash)

Relation MPIjoin_fromfiles(const char *filename, const char *filenamep, vector<int> z, vector<int> zp, int root=0);
Relation MPIjoin(Relation &rel, Relation &relp, int root=0);

Relation MPIautoJoin(Relation &rel, vector<int> &zp, int root=0);
Relation MPItriangle(Relation &rel, int root=0);

Relation hyperCubeMultiJoin(vector<Relation> toJoin, int root=0);
Relation hyperCubeTriangle(Relation &rel, int root=0);

#endif //MPIJOIN_H
