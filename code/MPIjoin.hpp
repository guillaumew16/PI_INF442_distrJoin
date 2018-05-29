#ifndef MPIJOIN_H
#define MPIJOIN_H

#include "relation.hpp"

Relation MPIjoin(const char *filename, const char *filenamep, vector<int> z, vector<int> zp);

#endif //MPIJOIN_H
