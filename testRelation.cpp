//#include <stdlib.h>

#include "relation.hpp"
#include <iostream>

using namespace std;

int main(int argc, char** argv) {
	Relation myRel("data_head/twitter.dat");
	Relation myRelp("data_head/facebook.dat");

	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	join(myRel, z, myRelp, zp);

	return 0;
}
