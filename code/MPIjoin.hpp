#ifndef MPIJOIN_H
#define MPIJOIN_H

#include "relation.hpp"

int h(unsigned int tohash, int len); //hash function (% or murmurhash)

Relation MPIjoin(Relation &rel, Relation &relp);

#endif //MPIJOIN_H
