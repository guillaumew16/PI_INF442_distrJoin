//#include <stdlib.h>

#include "relation.hpp"
#include <iostream>

using namespace std;

int main(int argc, char** argv) {

	//Relation twitterRel("../data/twitter.dat");
	//Relation facebookRel("../data/facebook.dat");
	//Relation dblpRel("data/dblp.dat");

	Relation twitterRel("../data_head/twitter.dat", 2);
	//Relation facebookRel("../data_head/facebook.dat");
	//Relation dblpRel("../data_head/dblp.dat");

	twitterRel.head(20);
	twitterRel.writeToFile("../output/copy.txt");

	/*
	vector<int> permutVect(2);
	permutVect[0] = 1;
	permutVect[1] = 0;
	Permutation permutation(permutVect);

	twitterRel.lexicoSort(permutation);
	twitterRel.head(20);
	twitterRel.writeToFile("../output/lexicoSort.txt");
	*/
	
	/*
	vector<int> z(2);
	z[0]=1;
	z[1]=3;
	vector<int> zp(2);
	zp[0]=3;
	zp[1]=4;

	Relation outRel = join(twitterRel, z, facebookRel, zp);
	outRel.writeToFile("../output/outRel.txt");
	*/
	

	return 0;
}
